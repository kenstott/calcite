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

import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.CacheKey;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
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
    // National Accounts & Growth
    public static final String GDP_CURRENT_USD = "NY.GDP.MKTP.CD";  // GDP (current US$)
    public static final String GDP_GROWTH = "NY.GDP.MKTP.KD.ZG";    // GDP growth (annual %)
    public static final String GDP_PER_CAPITA = "NY.GDP.PCAP.CD";   // GDP per capita (current US$)
    public static final String GNI_PER_CAPITA = "NY.GNP.PCAP.CD";   // GNI per capita (current US$)

    // Prices & Markets
    public static final String INFLATION_CPI = "FP.CPI.TOTL.ZG";    // Inflation, consumer prices
    // (annual %)

    // Labor Market
    public static final String UNEMPLOYMENT = "SL.UEM.TOTL.ZS";      // Unemployment, total (% of
    // labor force)
    public static final String LABOR_FORCE_TOTAL = "SL.TLF.TOTL.IN"; // Labor force, total
    public static final String LABOR_FORCE_PARTICIPATION = "SL.TLF.CACT.ZS"; // Labor force
    // participation rate (%)

    // Population
    public static final String POPULATION = "SP.POP.TOTL";           // Population, total
    public static final String LIFE_EXPECTANCY = "SP.DYN.LE00.IN";   // Life expectancy at birth
    // (years)

    // Trade & External Sector
    public static final String TRADE_BALANCE = "NE.RSB.GNFS.ZS";    // External balance on goods
    // and services (% of GDP)
    public static final String EXPORTS = "NE.EXP.GNFS.ZS";           // Exports of goods and
    // services (% of GDP)
    public static final String IMPORTS = "NE.IMP.GNFS.ZS";           // Imports of goods and
    // services (% of GDP)
    public static final String CURRENT_ACCOUNT = "BN.CAB.XOKA.GD.ZS"; // Current account balance
    // (% of GDP)
    public static final String FDI_NET_INFLOWS = "BX.KLT.DINV.WD.GD.ZS"; // FDI net inflows
    // (% of GDP)

    // Debt & Fiscal
    public static final String GOVT_DEBT = "GC.DOD.TOTL.GD.ZS";      // Central government debt,
    // total (% of GDP)
    public static final String EXTERNAL_DEBT = "DT.DOD.DECT.GN.ZS";  // External debt stocks
    // (% of GNI)
    public static final String GOVT_REVENUE = "GC.REV.XGRT.GD.ZS";   // Government revenue,
    // excluding grants (% of GDP)

    // Financial Sector
    public static final String INTEREST_RATE = "FR.INR.RINR";        // Real interest rate (%)
    public static final String EXCHANGE_RATE = "PA.NUS.FCRF";        // Official exchange rate
    // (LCU per US$)
    public static final String DOMESTIC_CREDIT = "FS.AST.DOMS.GD.ZS"; // Domestic credit to
    // private sector (% of GDP)
    public static final String MARKET_CAP = "CM.MKT.LCAP.GD.ZS";     // Market capitalization of
    // listed companies (% of GDP)

    // Education
    public static final String PRIMARY_ENROLLMENT = "SE.PRM.ENRR";   // Primary school enrollment
    // (% gross)
    public static final String SECONDARY_ENROLLMENT = "SE.SEC.ENRR"; // Secondary school enrollment
    // (% gross)
    public static final String EDUCATION_EXPENDITURE = "SE.XPD.TOTL.GD.ZS"; // Government
    // expenditure on education (% of GDP)

    // Health
    public static final String HEALTH_EXPENDITURE = "SH.XPD.CHEX.GD.ZS"; // Current health
    // expenditure (% of GDP)

    // Infrastructure & Investment
    public static final String GROSS_CAPITAL_FORMATION = "NE.GDI.TOTL.ZS"; // Gross capital
    // formation (% of GDP)

    // Inequality
    public static final String GINI_INDEX = "SI.POV.GINI";           // GINI index (World Bank
    // estimate)

    // Energy & Environment
    public static final String ENERGY_USE = "EG.USE.PCAP.KG.OE";     // Energy use (kg of oil
    // equivalent per capita)
    public static final String CO2_EMISSIONS = "EN.ATM.CO2E.PC";     // CO2 emissions (metric tons
    // per capita)
  }

  // Focus on major economies for comparison
  public static class Countries {
    // Economic groupings
    public static final List<String> G7 =
        Arrays.asList("USA", "JPN", "DEU", "GBR", "FRA", "ITA", "CAN");

    public static final List<String> G20 =
        Arrays.asList("USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "ITA", "BRA", "CAN",
            "KOR", "RUS", "AUS", "MEX", "IDN", "TUR", "SAU", "ARG", "ZAF");

    public static final List<String> MAJOR_ECONOMIES =
        Arrays.asList("USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "BRA", "ITA", "CAN",
            "KOR", "ESP", "AUS", "RUS", "MEX", "IDN", "NLD", "TUR", "CHE", "POL");

    // Regional aggregates (World Bank API codes)
    public static final List<String> REGIONAL_AGGREGATES =
        Arrays.asList(
            "EAP",  // East Asia & Pacific
            "ECA",  // Europe & Central Asia
            "LAC",  // Latin America & Caribbean
            "MNA",  // Middle East & North Africa
            "SAS",  // South Asia
            "SSA",  // Sub-Saharan Africa
            "EUU"); // European Union

    // Income-level aggregates (World Bank API codes)
    public static final List<String> INCOME_LEVELS =
        Arrays.asList(
            "HIC",  // High Income Countries
            "UMC",  // Upper-Middle Income
            "LMC",  // Lower-Middle Income
            "LIC"); // Low Income

    // Additional major economies (beyond G20)
    public static final List<String> ADDITIONAL_COUNTRIES =
        Arrays.asList(
            "NGA",  // Nigeria - Largest African economy
            "EGY",  // Egypt - Major MENA economy
            "PAK",  // Pakistan - South Asian economy
            "BGD",  // Bangladesh - Fast-growing South Asian economy
            "VNM",  // Vietnam - ASEAN manufacturing hub
            "THA",  // Thailand - ASEAN major economy
            "PHL",  // Philippines - ASEAN emerging market
            "MYS",  // Malaysia - ASEAN advanced economy
            "SGP",  // Singapore - Financial hub
            "NOR",  // Norway - High-income oil economy
            "SWE"); // Sweden - Nordic developed economy

    // World aggregate
    public static final String WORLD = "WLD";  // World total/average

    // Comprehensive list combining all country and regional codes
    public static final List<String> ALL_COUNTRIES = createAllCountriesList();

    private static List<String> createAllCountriesList() {
      List<String> all = new ArrayList<>();
      all.addAll(G20);
      all.addAll(REGIONAL_AGGREGATES);
      all.addAll(INCOME_LEVELS);
      all.addAll(ADDITIONAL_COUNTRIES);
      all.add(WORLD);
      return all;
    }
  }

  public WorldBankDataDownloader(String cacheDir,
      StorageProvider cacheStorageProvider,
      StorageProvider storageProvider) {
    this(cacheDir, cacheDir, cacheDir, cacheStorageProvider, storageProvider, null);
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
    List<String> indicatorCodes =
        Arrays.asList(
            // National Accounts & Growth
            Indicators.GDP_CURRENT_USD,
            Indicators.GDP_GROWTH,
            Indicators.GDP_PER_CAPITA,
            Indicators.GNI_PER_CAPITA,
            // Prices & Markets
            Indicators.INFLATION_CPI,
            // Labor Market
            Indicators.UNEMPLOYMENT,
            Indicators.LABOR_FORCE_TOTAL,
            Indicators.LABOR_FORCE_PARTICIPATION,
            // Population & Health
            Indicators.POPULATION,
            Indicators.LIFE_EXPECTANCY,
            Indicators.HEALTH_EXPENDITURE,
            // Trade & External Sector
            Indicators.TRADE_BALANCE,
            Indicators.EXPORTS,
            Indicators.IMPORTS,
            Indicators.CURRENT_ACCOUNT,
            Indicators.FDI_NET_INFLOWS,
            // Debt & Fiscal
            Indicators.GOVT_DEBT,
            Indicators.EXTERNAL_DEBT,
            Indicators.GOVT_REVENUE,
            // Financial Sector
            Indicators.INTEREST_RATE,
            Indicators.EXCHANGE_RATE,
            Indicators.DOMESTIC_CREDIT,
            Indicators.MARKET_CAP,
            // Education
            Indicators.PRIMARY_ENROLLMENT,
            Indicators.SECONDARY_ENROLLMENT,
            Indicators.EDUCATION_EXPENDITURE,
            // Infrastructure & Investment
            Indicators.GROSS_CAPITAL_FORMATION,
            // Inequality
            Indicators.GINI_INDEX,
            // Energy & Environment
            Indicators.ENERGY_USE,
            Indicators.CO2_EMISSIONS);

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
      try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
           InputStreamReader reader =
               new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
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
    List<Map<String, Object>> dataRecords = new ArrayList<>();
    for (Map<String, Object> ind : indicators) {
      Map<String, Object> record = new HashMap<>();
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
    List<PartitionedTableConfig.TableColumn> columns =
        AbstractEconDataDownloader.loadTableColumns("world_indicators");
    convertInMemoryToParquetViaDuckDB("world_indicators", columns, dataRecords, targetPath);
  }


}
