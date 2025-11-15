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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.CacheKey;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
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

  // Focus on major economies for comparison
  public static class Countries {
    public static final List<String> G7 = WorldBankCountryLoader.getG7();
    public static final List<String> G20 = WorldBankCountryLoader.getG20();
    public static final List<String> MAJOR_ECONOMIES = WorldBankCountryLoader.getMajorEconomies();
    public static final List<String> REGIONAL_AGGREGATES = WorldBankCountryLoader.getRegionalAggregates();
    public static final List<String> INCOME_LEVELS = WorldBankCountryLoader.getIncomeLevels();
    public static final List<String> ADDITIONAL_COUNTRIES = WorldBankCountryLoader.getAdditionalCountries();
    public static final String WORLD = WorldBankCountryLoader.getWorld();
    public static final List<String> ALL_COUNTRIES = WorldBankCountryLoader.getAllCountries();
  }

  public WorldBankDataDownloader(String cacheDir, String operatingDirectory, String parquetDir,
      StorageProvider cacheStorageProvider,
      StorageProvider storageProvider,
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

      Map<String, String> allParams = new HashMap<>(variables);
      allParams.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("world_indicators", allParams);

      if (!isParquetConvertedOrExists(cacheKey, rawPath, parquetPath)) {
        convertCachedJsonToParquet("world_indicators", variables);
        cacheManifest.markParquetConverted(cacheKey, parquetPath);
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

    // Download all indicators for comprehensive coverage
    List<String> indicatorCodes = WorldBankIndicatorLoader.getAllIndicatorCodes();

    // Use comprehensive country/region list for maximum coverage
    String countriesParam = String.join(";", Countries.ALL_COUNTRIES);

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
        jsonContent.getBytes(StandardCharsets.UTF_8));

    LOGGER.info("Worldbank indicators saved to: {} ({} records)", fullPath, indicators.size());
  }
}
