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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads and converts Bureau of Economic Analysis (BEA) data to Parquet format.
 * Provides detailed GDP components, personal income, trade statistics, and regional data.
 *
 * <p>Requires a BEA API key from <a href="https://apps.bea.gov/api/signup/">...</a>
 */
public class BeaDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BeaDataDownloader.class);
  private static final String BEA_API_BASE = "https://apps.bea.gov/api/data";

  private final String parquetDir;
  private final String apiKey;

  // BEA dataset names - comprehensive coverage of all available datasets
  public static class Datasets {
    public static final String NIPA = "NIPA";                               // National Income and Product Accounts
    public static final String NI_UNDERLYING_DETAIL = "NIUnderlyingDetail"; // Standard NI underlying detail tables
    public static final String MNE = "MNE";                                 // Multinational Enterprises
    public static final String FIXED_ASSETS = "FixedAssets";                // Standard Fixed Assets tables
    public static final String ITA = "ITA";                                 // International Transactions Accounts
    public static final String IIP = "IIP";                                 // International Investment Position
    public static final String INPUT_OUTPUT = "InputOutput";                // Input-Output Data
    public static final String INTL_SERV_TRADE = "IntlServTrade";           // International Services Trade
    public static final String INTL_SERV_STA = "IntlServSTA";              // International Services Supplied Through Affiliates
    public static final String GDP_BY_INDUSTRY = "GDPbyIndustry";           // GDP by Industry
    public static final String REGIONAL = "Regional";                       // Regional data sets
    public static final String UNDERLYING_GDP_BY_INDUSTRY = "UnderlyingGDPbyIndustry"; // Underlying GDP by Industry
    public static final String API_DATASET_METADATA = "APIDatasetMetaData"; // Metadata about other API datasets
  }

  // Key NIPA table IDs
  public static class NipaTables {
    public static final String GDP_COMPONENTS = "T10105";     // GDP and Components (Table 1.1.5)
    public static final String PERSONAL_INCOME = "58";   // Personal Income
    public static final String PERSONAL_CONSUMPTION = "66"; // Personal Consumption by Type
    public static final String GOVT_SPENDING = "86";     // Government Current Expenditures
    public static final String INVESTMENT = "51";        // Private Fixed Investment by Type
    public static final String EXPORTS_IMPORTS = "T40205B";  // Exports and Imports by Type
    public static final String CORPORATE_PROFITS = "45"; // Corporate Profits
    public static final String SAVINGS_RATE = "58";      // Personal Saving Rate
  }

  // Key ITA (International Transactions Accounts) indicators
  public static class ItaIndicators {
    public static final String BALANCE_GOODS = "BalGds";                    // Balance on goods
    public static final String BALANCE_SERVICES = "BalServ";                // Balance on services
    public static final String BALANCE_GOODS_SERVICES = "BalGdsServ";       // Balance on goods and services
    public static final String BALANCE_CURRENT_ACCOUNT = "BalCurrAcct";     // Balance on current account
    public static final String BALANCE_CAPITAL_ACCOUNT = "BalCapAcct";      // Balance on capital account
    public static final String BALANCE_PRIMARY_INCOME = "BalPrimInc";       // Balance on primary income
    public static final String BALANCE_SECONDARY_INCOME = "BalSecInc";      // Balance on secondary income
    public static final String EXPORTS_GOODS = "ExpGds";                    // Exports of goods
    public static final String IMPORTS_GOODS = "ImpGds";                    // Imports of goods
    public static final String EXPORTS_SERVICES = "ExpServ";                // Exports of services
    public static final String IMPORTS_SERVICES = "ImpServ";                // Imports of services
  }

  // Key GDP by Industry table IDs
  public static class GdpByIndustryTables {
    public static final String VALUE_ADDED_BY_INDUSTRY = "1";               // Gross Output and Value Added by Industry
    public static final String GDP_BY_INDUSTRY_ANNUAL = "2";                // Value Added by Industry as a Percentage of GDP
    public static final String EMPLOYMENT_BY_INDUSTRY = "3";                // Full-Time and Part-Time Employees by Industry
    public static final String COMPENSATION_BY_INDUSTRY = "4";              // Compensation by Industry
  }

  public BeaDataDownloader(String cacheDir, String parquetDir, String apiKey, StorageProvider cacheStorageProvider, StorageProvider storageProvider) {
    this(cacheDir, cacheDir, parquetDir, apiKey, cacheStorageProvider, storageProvider, null);
  }

  public BeaDataDownloader(String cacheDir, String operatingDirectory, String parquetDir, String apiKey, StorageProvider cacheStorageProvider, StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, cacheDir, cacheStorageProvider, storageProvider, sharedManifest);
    this.parquetDir = parquetDir;
    this.apiKey = apiKey;
  }

  // Temporary compatibility constructor - creates LocalFileStorageProvider internally
  public BeaDataDownloader(String cacheDir, String apiKey) {
    super(cacheDir, org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir), org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir));
    this.parquetDir = cacheDir; // For compatibility, use same dir
    this.apiKey = apiKey;
  }

  @Override protected String getTableName() {
    return "gdp_components";
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
   * Downloads all BEA data for the specified year range.
   */
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    // Download all datasets year by year to match expected directory structure
    for (int year = startYear; year <= endYear; year++) {
      // Download GDP components
      downloadGdpComponentsForYear(year);

      // Download regional income for single year
      try {
        LOGGER.debug("About to call downloadRegionalIncomeForYear for year {}", year);
        downloadRegionalIncomeForYear(year);
        LOGGER.debug("Successfully completed downloadRegionalIncomeForYear for year {}", year);
      } catch (Exception e) {
        LOGGER.error("Failed to download regional income data for year {}: {}", year, e.getMessage(), e);
      }

      // Download trade statistics for single year
      try {
        downloadTradeStatisticsForYear(year);
      } catch (Exception e) {
        LOGGER.warn("Failed to download trade statistics for year {}: {}", year, e.getMessage());
      }

      // Download ITA data for single year
      try {
        downloadItaDataForYear(year);
      } catch (Exception e) {
        LOGGER.warn("Failed to download ITA data for year {}: {}", year, e.getMessage());
      }

      // Download industry GDP for single year
      try {
        downloadIndustryGdpForYear(year);
      } catch (Exception e) {
        LOGGER.warn("Failed to download industry GDP data for year {}: {}", year, e.getMessage());
      }

      // Download state GDP for single year
      try {
        downloadStateGdpForYear(year);
      } catch (Exception e) {
        LOGGER.warn("Failed to download state GDP data for year {}: {}", year, e.getMessage());
      }
    }
  }

  /**
   * Downloads GDP components for a specific year.
   */
  public void downloadGdpComponentsForYear(int year) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }

    // Check if this is a future year - BEA annual data not available for future years
    int currentYear = LocalDate.now().getYear();
    if (year > currentYear || (year == currentYear && LocalDate.now().getMonthValue() < 2)) {
      LOGGER.debug("Skipping BEA GDP components for year {} - annual data not yet available", year);
      createEmptyGdpComponentsFile(year, "Annual data not yet available");
      return;
    }

    // Check cache before downloading
    Map<String, String> cacheParams = new HashMap<>();
    // Don't include redundant params - year is already a parameter, type is the dataType

    if (cacheManifest.isCached("gdp_components", year, cacheParams)) {
      LOGGER.debug("Found cached GDP components for year {} - skipping download", year);
      return;
    }

    // Check if file exists using StorageProvider and update manifest (Quarterly frequency)
    String relativePath = buildPartitionPath("gdp_components", DataFrequency.QUARTERLY, year) + "/gdp_components.json";
    try {
      if (cacheStorageProvider.exists(relativePath)) {
        LOGGER.debug("Found existing GDP components file for year {} - updating manifest", year);
        cacheManifest.markCached("gdp_components", year, cacheParams, relativePath, 0L);
        cacheManifest.save(operatingDirectory);
        return;
      }
    } catch (Exception e) {
      LOGGER.debug("Could not check if file exists: {}", e.getMessage());
    }

    // Directory creation handled automatically by StorageProvider when writing files

    List<GdpComponent> components = new ArrayList<>();

    // Download GDP components (Table 1)
    String params =
        String.format("UserID=%s&method=GetData&datasetname=%s&TableName=%s&Frequency=A&Year=%d&ResultFormat=JSON", apiKey, Datasets.NIPA, NipaTables.GDP_COMPONENTS, year);

    String url = BEA_API_BASE + "?" + params;

    LOGGER.debug("Downloading BEA GDP components for year {}", year);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      LOGGER.warn("BEA API request failed for year {} with status: {}", year, response.statusCode());
      return;
    }

    LOGGER.debug("BEA API GDP components request URL: {}", url);
    JsonNode root = MAPPER.readTree(response.body());
    LOGGER.debug("BEA API GDP components response for year {}: {}", year,
                 response.body().length() > 500 ? response.body().substring(0, 500) + "..." : response.body());

    // Check for API errors first
    if (root.has("BEAAPI")) {
      JsonNode beaApi = root.get("BEAAPI");
      if (beaApi.has("Request") && beaApi.get("Request").has("RequestParam")) {
        LOGGER.debug("BEA API Request parameters: {}", beaApi.get("Request").get("RequestParam"));
      }
      if (beaApi.has("Error")) {
        JsonNode error = beaApi.get("Error");
        LOGGER.warn("BEA API error for GDP components year {}: {}", year, error);

        // Create empty file for years where data is not available
        String errorMsg = error.has("APIErrorDescription") ?
            error.get("APIErrorDescription").asText() : "Data not available";
        createEmptyGdpComponentsFile(year, errorMsg);
        return;
      }
    }

    JsonNode results = root.path("BEAAPI").path("Results");

    if (results != null && results.has("Data")) {
      JsonNode dataArray = results.get("Data");
      LOGGER.debug("BEA API returned {} data records for GDP components year {}",
                   dataArray.size(), year);
      if (dataArray != null && dataArray.isArray()) {
        for (JsonNode record : dataArray) {
          GdpComponent component = new GdpComponent();
          component.lineNumber = record.get("LineNumber").asInt();
          component.lineDescription = record.get("LineDescription").asText();
          component.seriesCode = record.get("SeriesCode").asText();
          component.year = record.get("TimePeriod").asInt();

          // Parse value, handling special cases
          String dataValue = record.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty()) {
            try {
              component.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              continue; // Skip invalid values
            }
          } else {
            continue;
          }

          component.units = "Billions of dollars";
          component.tableId = NipaTables.GDP_COMPONENTS;
          component.frequency = "A";

          components.add(component);
        }
      }
    }

    // Save raw JSON data to local cache
    // jsonFile already defined above at line 274
    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> componentsData = getMaps(components);

    data.put("components", componentsData);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);

    String jsonContent = MAPPER.writeValueAsString(data);

    // Write using StorageProvider - resolve path against cacheDirectory (which includes source=econ)
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, jsonContent.getBytes(StandardCharsets.UTF_8));

    LOGGER.debug("GDP components saved to: {} ({} records)", relativePath, components.size());

    // Mark as cached in manifest
    cacheManifest.markCached("gdp_components", year, cacheParams, relativePath, 0L);
    cacheManifest.save(operatingDirectory);
  }

  private static List<Map<String, Object>> getMaps(List<GdpComponent> components) {
    List<Map<String, Object>> componentsData = new ArrayList<>();

    for (GdpComponent component : components) {
      Map<String, Object> compData = new HashMap<>();
      compData.put("table_id", component.tableId);
      compData.put("line_number", component.lineNumber);
      compData.put("line_description", component.lineDescription);
      compData.put("series_code", component.seriesCode);
      compData.put("year", component.year);
      compData.put("value", component.value);
      compData.put("units", component.units);
      compData.put("frequency", component.frequency);
      componentsData.add(compData);
    }
    return componentsData;
  }

  /**
   * Downloads GDP components using default date range.
   */
  public String downloadGdpComponents() throws IOException, InterruptedException {
    return downloadGdpComponents(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads detailed GDP components data.
   * @return The storage path (local or S3) where the data was saved
   */
  public String downloadGdpComponents(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }

    // Build RELATIVE path (StorageProvider will add base path)
    String relativePath =
        String.format("type=gdp_components/year_range=%d_%d/gdp_components.parquet", startYear, endYear);

    List<GdpComponent> components = new ArrayList<>();

    // Build year list for API request
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    String yearParam = String.join(",", years);

    // Download GDP components (Table 1)
    String params =
        String.format("UserID=%s&method=GetData&datasetname=%s&TableName=%s&Frequency=A&Year=%s&ResultFormat=JSON", apiKey, Datasets.NIPA, NipaTables.GDP_COMPONENTS, yearParam);

    String url = BEA_API_BASE + "?" + params;

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("BEA API request failed with status: " + response.statusCode());
    }

    JsonNode root = MAPPER.readTree(response.body());
    JsonNode results = root.get("BEAAPI").get("Results");

    if (results != null && results.has("Data")) {
      JsonNode dataArray = results.get("Data");
      if (dataArray != null && dataArray.isArray()) {
        for (JsonNode record : dataArray) {
          GdpComponent component = new GdpComponent();
          component.lineNumber = record.get("LineNumber").asInt();
          component.lineDescription = record.get("LineDescription").asText();
          component.seriesCode = record.get("SeriesCode").asText();
          component.year = record.get("TimePeriod").asInt();

          // Parse value, handling special cases
          String dataValue = record.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty()) {
            try {
              component.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              continue; // Skip invalid values
            }
          } else {
            continue;
          }

          component.units = "Billions of dollars";
          component.tableId = NipaTables.GDP_COMPONENTS;
          component.frequency = "A";

          components.add(component);
        }
      }
    }

    // Also download personal consumption details
    Thread.sleep(100); // Rate limiting

    params =
        String.format("UserID=%s&method=GetData&datasetname=%s&TableName=%s&Frequency=A&Year=%s&ResultFormat=JSON", apiKey, Datasets.NIPA, NipaTables.PERSONAL_CONSUMPTION, yearParam);

    url = BEA_API_BASE + "?" + params;
    request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();

    response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      root = MAPPER.readTree(response.body());
      results = root.get("BEAAPI").get("Results");

      if (results != null && results.has("Data")) {
        JsonNode dataArray = results.get("Data");
        if (dataArray != null && dataArray.isArray()) {
          for (JsonNode record : dataArray) {
            GdpComponent component = new GdpComponent();
            component.lineNumber = record.get("LineNumber").asInt();
            component.lineDescription = record.get("LineDescription").asText();
            component.seriesCode = record.get("SeriesCode").asText();
            component.year = record.get("TimePeriod").asInt();

            String dataValue = record.get("DataValue").asText();
            if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty()) {
              try {
                component.value = Double.parseDouble(dataValue.replace(",", ""));
              } catch (NumberFormatException e) {
                continue;
              }
            } else {
              continue;
            }

            component.units = "Billions of dollars";
            component.tableId = NipaTables.PERSONAL_CONSUMPTION;
            component.frequency = "A";

            components.add(component);
          }
        }
      }
    }

    // Convert to Parquet
    writeGdpComponentsParquet(components, relativePath);

    LOGGER.debug("GDP components saved to: {} ({} records)", relativePath, components.size());
    return relativePath;
  }

  /**
   * Downloads regional income data using default date range.
   */
  public void downloadRegionalIncome() throws IOException, InterruptedException {
    downloadRegionalIncome(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads regional income data for a single year.
   */
  public void downloadRegionalIncomeForYear(int year) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      LOGGER.error("BEA API key is missing - cannot download regional income data");
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }

    // Check cache first
    Map<String, String> cacheParams = new HashMap<>();
    // Don't include redundant params - year is already a parameter, type is the dataType

    if (cacheManifest.isCached("regional_income", year, cacheParams)) {
      LOGGER.debug("Found cached regional income data for year {} - skipping download", year);
      return;
    }

    LOGGER.debug("Downloading BEA regional income data for year {} with API key: {}...", year, apiKey.substring(0, 4));

    // Directory creation handled automatically by StorageProvider when writing files

    List<RegionalIncome> incomeData = new ArrayList<>();

    // Regional API requires separate calls for each LineCode (1=Income, 2=Population, 3=Per Capita)
    String[] lineCodes = {"1", "2", "3"};

    for (String lineCode : lineCodes) {
      LOGGER.debug("Downloading regional income data for year {} LineCode {}", year, lineCode);

      String params =
          String.format("UserID=%s&method=GetData&datasetname=%s&TableName=SAINC1&LineCode=%s&GeoFips=STATE&Year=%d&ResultFormat=JSON", apiKey, Datasets.REGIONAL, lineCode, year);

      String url = BEA_API_BASE + "?" + params;

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        LOGGER.warn("BEA regional income API request failed for year {} LineCode {} with status: {}", year, lineCode, response.statusCode());
        continue;
      }

      JsonNode root = MAPPER.readTree(response.body());
      JsonNode results = root.get("BEAAPI").get("Results");

      // Check for API errors
      if (results != null && results.has("Error")) {
        JsonNode error = results.get("Error");
        LOGGER.warn("BEA API error for year {} LineCode {}: {} - {}", year, lineCode,
                   error.get("APIErrorCode").asText(), error.get("APIErrorDescription").asText());
        continue;
      }

      if (results != null && results.has("Data")) {
        JsonNode dataArray = results.get("Data");
        if (dataArray != null && dataArray.isArray()) {
          for (JsonNode record : dataArray) {
            try {
              RegionalIncome income = new RegionalIncome();

              // Get fields with null checks
              JsonNode geoFipsNode = record.get("GeoFips");
              JsonNode geoNameNode = record.get("GeoName");
              JsonNode codeNode = record.get("Code");
              JsonNode lineCodeNode = record.get("LineCode");  // Try both Code and LineCode
              JsonNode descNode = record.get("Description");
              JsonNode timePeriodNode = record.get("TimePeriod");
              JsonNode dataValueNode = record.get("DataValue");

              if (geoFipsNode == null || geoNameNode == null || timePeriodNode == null || dataValueNode == null) {
                LOGGER.debug("Skipping record with missing required fields");
                continue;
              }

              income.geoFips = geoFipsNode.asText();
              income.geoName = geoNameNode.asText();

              // Handle LineCode - BEA Regional uses LineCode, not Code
              if (lineCodeNode != null) {
                income.lineCode = lineCodeNode.asText();
              } else if (codeNode != null) {
                income.lineCode = codeNode.asText();
              } else {
                income.lineCode = lineCode;  // Use the LineCode from the request
              }

              // Description might be in different field or not present
              if (descNode != null) {
                income.lineDescription = descNode.asText();
              } else {
                // Set description based on line code
                if ("1".equals(income.lineCode)) {
                  income.lineDescription = "Personal income (thousands of dollars)";
                } else if ("2".equals(income.lineCode)) {
                  income.lineDescription = "Population (persons)";
                } else if ("3".equals(income.lineCode)) {
                  income.lineDescription = "Per capita personal income (dollars)";
                } else {
                  income.lineDescription = "Line " + income.lineCode;
                }
              }

              income.year = Integer.parseInt(timePeriodNode.asText());

              String dataValue = dataValueNode.asText();
              if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
                try {
                  income.value = Double.parseDouble(dataValue.replace(",", ""));
                } catch (NumberFormatException e) {
                  continue;
                }
              } else {
                continue;
              }

              // Set units based on line code
              if ("1".equals(income.lineCode)) {
                income.units = "Thousands of dollars";
                income.metric = "Total Personal Income";
              } else if ("2".equals(income.lineCode)) {
                income.units = "Persons";
                income.metric = "Population";
              } else if ("3".equals(income.lineCode)) {
                income.units = "Dollars";
                income.metric = "Per Capita Personal Income";
              }

              incomeData.add(income);
            } catch (Exception e) {
              LOGGER.debug("Failed to parse regional income record: {}", e.getMessage());
            }
          }
        }
      }
    }

    // Save as JSON to cache using StorageProvider (Annual frequency)
    String relativePath = buildPartitionPath("regional_income", DataFrequency.ANNUAL, year) + "/regional_income.json";
    LOGGER.debug("Preparing to save {} regional income records to {}", incomeData.size(), relativePath);

    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> incomeList = new ArrayList<>();

    for (RegionalIncome income : incomeData) {
      Map<String, Object> incomeMap = new HashMap<>();
      incomeMap.put("geo_fips", income.geoFips);
      incomeMap.put("geo_name", income.geoName);
      incomeMap.put("metric", income.metric);
      incomeMap.put("line_code", income.lineCode);
      incomeMap.put("line_description", income.lineDescription);
      incomeMap.put("year", income.year);
      incomeMap.put("value", income.value);
      incomeMap.put("units", income.units);
      incomeList.add(incomeMap);
    }

    data.put("regional_income", incomeList);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);

    String jsonContent = MAPPER.writeValueAsString(data);
    // Resolve path against cacheDirectory (which includes source=econ) before writing
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, jsonContent.getBytes(StandardCharsets.UTF_8));

    LOGGER.debug("Regional income data saved to: {} ({} records)", relativePath, incomeData.size());

    // Mark as cached in manifest (use relative path)
    cacheManifest.markCached("regional_income", year, cacheParams, relativePath, 0L);
    cacheManifest.save(operatingDirectory);
  }

  /**
   * Downloads regional personal income data by state.
   */
  public void downloadRegionalIncome(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }

    LOGGER.info("Downloading BEA regional income data for {}-{}", startYear, endYear);

    // Build RELATIVE path (StorageProvider will add base path)
    String relativePath =
        String.format("type=regional_income/year_range=%d_%d/regional_income.parquet", startYear, endYear);

    List<RegionalIncome> incomeData = new ArrayList<>();

    // Build year list
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    String yearParam = String.join(",", years);

    // Download state personal income data
    // LineCode 1 = Total Personal Income, 2 = Population, 3 = Per Capita Income
    String params =
        String.format("UserID=%s&method=GetData&datasetname=%s&TableName=SAINC1&LineCode=1,2,3&GeoFips=STATE&Year=%s&ResultFormat=JSON", apiKey, Datasets.REGIONAL, yearParam);

    String url = BEA_API_BASE + "?" + params;

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("BEA API request failed with status: " + response.statusCode());
    }

    JsonNode root = MAPPER.readTree(response.body());
    JsonNode results = root.get("BEAAPI").get("Results");

    if (results != null && results.has("Data")) {
      JsonNode dataArray = results.get("Data");
      if (dataArray != null && dataArray.isArray()) {
        for (JsonNode record : dataArray) {
          RegionalIncome income = new RegionalIncome();
          income.geoFips = record.get("GeoFips").asText();
          income.geoName = record.get("GeoName").asText();
          income.lineCode = record.get("Code").asText();
          income.lineDescription = record.get("Description").asText();
          income.year = Integer.parseInt(record.get("TimePeriod").asText());

          String dataValue = record.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
            try {
              income.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              continue;
            }
          } else {
            continue;
          }

          // Set units based on line code
          if ("1".equals(income.lineCode)) {
            income.units = "Thousands of dollars";
            income.metric = "Total Personal Income";
          } else if ("2".equals(income.lineCode)) {
            income.units = "Persons";
            income.metric = "Population";
          } else if ("3".equals(income.lineCode)) {
            income.units = "Dollars";
            income.metric = "Per Capita Personal Income";
          }

          incomeData.add(income);
        }
      }
    }

    // Convert to Parquet
    writeRegionalIncomeParquet(incomeData, relativePath);

    LOGGER.debug("Regional income data saved to: {} ({} records)", relativePath, incomeData.size());
  }

  /**
   * Downloads trade statistics using default date range.
   */
  public void downloadTradeStatistics() throws IOException, InterruptedException {
    downloadTradeStatistics(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads trade statistics for a single year.
   */
  public void downloadTradeStatisticsForYear(int year) throws IOException, InterruptedException {
    LOGGER.info("=== PHASE 4 DEBUG: downloadTradeStatisticsForYear() called for year {} ===", year);

    if (apiKey == null || apiKey.isEmpty()) {
      LOGGER.error("PHASE 4 DEBUG: BEA API key is NULL or EMPTY!");
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }
    LOGGER.info("PHASE 4 DEBUG: BEA API key is present");

    // Check if parquet file already exists (skip download if already converted)
    String tradeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/trade_statistics.parquet");
    LOGGER.info("PHASE 4 DEBUG: Checking if parquet exists at: {}", tradeParquetPath);
    if (storageProvider.exists(tradeParquetPath)) {
      LOGGER.info("PHASE 4 DEBUG: *** SKIPPING *** Found existing trade statistics parquet for year {} at {}", year, tradeParquetPath);
      return;
    }
    LOGGER.info("PHASE 4 DEBUG: Parquet does NOT exist, continuing...");

    // Check cache first
    Map<String, String> cacheParams = new HashMap<>();
    // Don't include redundant params - year is already a parameter, type is the dataType

    LOGGER.info("PHASE 4 DEBUG: Checking cache manifest for trade_statistics year {}", year);
    if (cacheManifest.isCached("trade_statistics", year, cacheParams)) {
      LOGGER.info("PHASE 4 DEBUG: *** SKIPPING *** Found cached trade statistics for year {}", year);
      return;
    }
    LOGGER.info("PHASE 4 DEBUG: Not in cache, proceeding with download...");

    LOGGER.info("PHASE 4 DEBUG: Starting HTTP download for BEA trade statistics year {}", year);

    // Directory creation handled automatically by StorageProvider when writing files

    List<TradeStatistic> tradeData = new ArrayList<>();

    // Download exports and imports (Table 125) for single year
    String params =
        String.format("UserID=%s&method=GetData&datasetname=%s&TableName=%s&Frequency=A&Year=%d&ResultFormat=JSON", apiKey, Datasets.NIPA, NipaTables.EXPORTS_IMPORTS, year);

    String url = BEA_API_BASE + "?" + params;

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      LOGGER.warn("BEA trade statistics API request failed for year {} with status: {}", year, response.statusCode());
      return;
    }

    JsonNode root = MAPPER.readTree(response.body());
    JsonNode results = root.get("BEAAPI").get("Results");

    if (results != null && results.has("Data")) {
      JsonNode dataArray = results.get("Data");
      if (dataArray != null && dataArray.isArray()) {
        for (JsonNode record : dataArray) {
          TradeStatistic trade = new TradeStatistic();
          trade.tableId = NipaTables.EXPORTS_IMPORTS;
          trade.lineNumber = record.get("LineNumber").asInt();
          trade.lineDescription = record.get("LineDescription").asText();
          trade.seriesCode = record.get("SeriesCode").asText();
          trade.year = record.get("TimePeriod").asInt();
          trade.frequency = "A";
          trade.units = "Billions of dollars";

          // Parse value, handling special cases
          String dataValue = record.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
            try {
              trade.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              continue; // Skip invalid values
            }
          } else {
            continue;
          }

          // Determine trade type and category from line description
          parseTradeTypeAndCategory(trade);

          tradeData.add(trade);
        }
      }
    }

    // Calculate trade balances for matching export/import pairs
    calculateTradeBalances(tradeData);

    // Save as JSON to cache using StorageProvider
    // Use type=indicators pattern to match where converters expect files
    String relativePath = "type=indicators/year=" + year + "/trade_statistics.json";
    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> tradeList = new ArrayList<>();

    for (TradeStatistic trade : tradeData) {
      Map<String, Object> tradeMap = new HashMap<>();
      tradeMap.put("table_id", trade.tableId);
      tradeMap.put("line_number", trade.lineNumber);
      tradeMap.put("line_description", trade.lineDescription);
      tradeMap.put("series_code", trade.seriesCode);
      tradeMap.put("year", trade.year);
      tradeMap.put("value", trade.value);
      tradeMap.put("units", trade.units);
      tradeMap.put("frequency", trade.frequency);
      tradeMap.put("trade_type", trade.tradeType);
      tradeMap.put("category", trade.category);
      tradeMap.put("trade_balance", trade.tradeBalance);
      tradeList.add(tradeMap);
    }

    data.put("trade_statistics", tradeList);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);

    String jsonContent = MAPPER.writeValueAsString(data);
    // Resolve path against cacheDirectory (which includes source=econ) before writing
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, jsonContent.getBytes(StandardCharsets.UTF_8));

    LOGGER.debug("Trade statistics saved to: {} ({} records)", relativePath, tradeData.size());

    // Mark as cached in manifest (use relative path)
    cacheManifest.markCached("trade_statistics", year, cacheParams, relativePath, 0L);
    cacheManifest.save(operatingDirectory);
  }

  /**
   * Downloads trade statistics (exports and imports) from BEA Table 125.
   * Provides detailed breakdown of exports and imports by category.
   */
  public void downloadTradeStatistics(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }

    LOGGER.info("Downloading BEA trade statistics for {}-{}", startYear, endYear);

    // Build RELATIVE path (StorageProvider will add base path)
    String relativePath =
        String.format("type=trade_statistics/year_range=%d_%d/trade_statistics.parquet", startYear, endYear);

    List<TradeStatistic> tradeData = new ArrayList<>();

    // Build year list for API request
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    String yearParam = String.join(",", years);

    // Download exports and imports (Table 125)
    String params =
        String.format("UserID=%s&method=GetData&datasetname=%s&TableName=%s&Frequency=A&Year=%s&ResultFormat=JSON", apiKey, Datasets.NIPA, NipaTables.EXPORTS_IMPORTS, yearParam);

    String url = BEA_API_BASE + "?" + params;

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("BEA API request failed with status: " + response.statusCode());
    }

    JsonNode root = MAPPER.readTree(response.body());
    JsonNode results = root.get("BEAAPI").get("Results");

    if (results != null && results.has("Data")) {
      JsonNode dataArray = results.get("Data");
      if (dataArray != null && dataArray.isArray()) {
        for (JsonNode record : dataArray) {
          TradeStatistic trade = new TradeStatistic();
          trade.tableId = NipaTables.EXPORTS_IMPORTS;
          trade.lineNumber = record.get("LineNumber").asInt();
          trade.lineDescription = record.get("LineDescription").asText();
          trade.seriesCode = record.get("SeriesCode").asText();
          trade.year = record.get("TimePeriod").asInt();
          trade.frequency = "A";
          trade.units = "Billions of dollars";

          // Parse value, handling special cases
          String dataValue = record.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
            try {
              trade.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              continue; // Skip invalid values
            }
          } else {
            continue;
          }

          // Determine trade type and category from line description
          parseTradeTypeAndCategory(trade);

          tradeData.add(trade);
        }
      }
    }

    // Calculate trade balances for matching export/import pairs
    calculateTradeBalances(tradeData);

    // Convert to Parquet
    writeTradeStatisticsParquet(tradeData, relativePath);

    LOGGER.debug("Trade statistics saved to: {} ({} records)", relativePath, tradeData.size());
  }

  /**
   * Parses trade type and category from BEA line description.
   * Maps line descriptions to export/import categories.
   */
  private void parseTradeTypeAndCategory(TradeStatistic trade) {
    String desc = trade.lineDescription.toLowerCase();

    // Determine if this is an export or import based on line description
    if (desc.contains("export")) {
      trade.tradeType = "Exports";
    } else if (desc.contains("import")) {
      trade.tradeType = "Imports";
    } else if (trade.lineNumber >= 1 && trade.lineNumber <= 50) {
      // Lines 1-50 are typically exports in Table 125
      trade.tradeType = "Exports";
    } else if (trade.lineNumber >= 51 && trade.lineNumber <= 100) {
      // Lines 51-100 are typically imports in Table 125
      trade.tradeType = "Imports";
    } else {
      trade.tradeType = "Other";
    }

    // Parse category from line description
    if (desc.contains("goods")) {
      trade.category = "Goods";
    } else if (desc.contains("services")) {
      trade.category = "Services";
    } else if (desc.contains("food")) {
      trade.category = "Food";
    } else if (desc.contains("industrial supplies") || desc.contains("materials")) {
      trade.category = "Industrial Supplies";
    } else if (desc.contains("capital goods") || desc.contains("machinery")) {
      trade.category = "Capital Goods";
    } else if (desc.contains("automotive") || desc.contains("vehicle")) {
      trade.category = "Automotive";
    } else if (desc.contains("consumer goods")) {
      trade.category = "Consumer Goods";
    } else if (desc.contains("petroleum") || desc.contains("oil")) {
      trade.category = "Petroleum";
    } else if (desc.contains("travel")) {
      trade.category = "Travel Services";
    } else if (desc.contains("transport")) {
      trade.category = "Transportation";
    } else if (desc.contains("financial")) {
      trade.category = "Financial Services";
    } else if (desc.contains("intellectual property") || desc.contains("royalties")) {
      trade.category = "Intellectual Property";
    } else {
      // Use the first few words as category
      String[] words = trade.lineDescription.split("\\s+");
      if (words.length >= 2) {
        trade.category = words[0] + " " + words[1];
      } else {
        trade.category = "Other";
      }
    }
  }

  /**
   * Calculates trade balances for matching export/import categories.
   */
  private void calculateTradeBalances(List<TradeStatistic> tradeData) {
    // Group by year and category to calculate balances
    Map<String, Map<String, Double>> exports = new HashMap<>();
    Map<String, Map<String, Double>> imports = new HashMap<>();

    // Separate exports and imports
    for (TradeStatistic trade : tradeData) {
      String key = trade.year + "_" + trade.category;

      if ("Exports".equals(trade.tradeType)) {
        exports.computeIfAbsent(key, k -> new HashMap<>()).put(trade.category, trade.value);
      } else if ("Imports".equals(trade.tradeType)) {
        imports.computeIfAbsent(key, k -> new HashMap<>()).put(trade.category, trade.value);
      }
    }

    // Calculate trade balance for each record
    for (TradeStatistic trade : tradeData) {
      String key = trade.year + "_" + trade.category;
      Double exportValue = exports.getOrDefault(key, new HashMap<>()).get(trade.category);
      Double importValue = imports.getOrDefault(key, new HashMap<>()).get(trade.category);

      if (exportValue != null && importValue != null) {
        trade.tradeBalance = exportValue - importValue;
      } else if ("Exports".equals(trade.tradeType) && importValue != null) {
        trade.tradeBalance = trade.value - importValue;
      } else if ("Imports".equals(trade.tradeType) && exportValue != null) {
        trade.tradeBalance = exportValue - trade.value;
      } else {
        trade.tradeBalance = 0.0; // No matching pair found
      }
    }
  }

  public void writeTradeStatisticsParquet(List<TradeStatistic> tradeStats, String targetFilePath) throws IOException {
    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (TradeStatistic trade : tradeStats) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("table_id", trade.tableId);
      record.put("line_number", trade.lineNumber);
      record.put("line_description", trade.lineDescription);
      record.put("series_code", trade.seriesCode);
      // year comes from partition key (year=*/), not parquet file
      record.put("value", trade.value);
      record.put("units", trade.units);
      record.put("frequency", trade.frequency);
      record.put("trade_type", trade.tradeType);
      record.put("category", trade.category);
      record.put("trade_balance", trade.tradeBalance);
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("trade_statistics");
    storageProvider.writeAvroParquet(targetFilePath, columns, dataRecords, "TradeStatistic", "TradeStatistic");
    LOGGER.debug("Trade statistics Parquet written: {} ({} records)", targetFilePath, tradeStats.size());
  }

  /**
   * Downloads International Transactions Accounts data using default date range.
   */
  public String downloadItaData() throws IOException, InterruptedException {
    return downloadItaData(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads ITA data for a single year.
   */
  public void downloadItaDataForYear(int year) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }

    // Check if parquet file already exists (skip download if already converted)
    String itaParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/ita_data.parquet");
    if (storageProvider.exists(itaParquetPath)) {
      LOGGER.debug("Found existing ITA parquet for year {} - skipping download", year);
      return;
    }

    // Check cache first
    Map<String, String> cacheParams = new HashMap<>();
    // Don't include redundant params - year is already a parameter, type is the dataType

    if (cacheManifest.isCached("ita_data", year, cacheParams)) {
      LOGGER.debug("Found cached ITA data for year {} - skipping download", year);
      return;
    }

    // Directory creation handled automatically by StorageProvider when writing files

    LOGGER.debug("Downloading BEA ITA data for year {}", year);

    List<ItaData> itaRecords = new ArrayList<>();

    // Download key ITA indicators for single year
    String[] indicators = {
        ItaIndicators.BALANCE_GOODS,
        ItaIndicators.BALANCE_SERVICES,
        ItaIndicators.BALANCE_GOODS_SERVICES,
        ItaIndicators.BALANCE_CURRENT_ACCOUNT
    };

    for (String indicator : indicators) {
      // Rate limiting to avoid 429 errors
      Thread.sleep(500);

      String params =
          String.format("UserID=%s&method=GetData&DataSetName=%s&Indicator=%s&AreaOrCountry=AllCountries&Frequency=A&Year=%d&ResultFormat=JSON", apiKey, Datasets.ITA, indicator, year);

      String url = BEA_API_BASE + "?" + params;

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        LOGGER.warn("ITA API request failed for indicator {} year {} with status: {}", indicator, year, response.statusCode());
        continue;
      }

      LOGGER.debug("ITA API URL: {}", url);
      LOGGER.debug("ITA API Response (first 500 chars): {}", response.body().substring(0, Math.min(500, response.body().length())));

      JsonNode root = MAPPER.readTree(response.body());
      JsonNode results = root.get("BEAAPI").get("Results");

      LOGGER.debug("ITA API Results node exists: {}", results != null);
      if (results != null) {
        LOGGER.debug("ITA API Results has Error: {}", results.has("Error"));
        LOGGER.debug("ITA API Results has Data: {}", results.has("Data"));
        if (results.has("Error")) {
          JsonNode error = results.get("Error");
          String errorCode = error.path("APIErrorCode").asText();
          String errorDesc = error.path("APIErrorDescription").asText();
          LOGGER.error("BEA ITA API Error {}: {} for indicator {} year {}", errorCode, errorDesc, indicator, year);

          // Check for invalid API key
          if ("1".equals(errorCode) && errorDesc.contains("Invalid API UserId")) {
            LOGGER.error("BEA API key is invalid or missing. Please set BEA_API_KEY environment variable with a valid key from https://apps.bea.gov/api/signup/");
            // Continue to next indicator rather than failing completely
            continue;
          }
        }
      }

      if (results != null && results.has("Data")) {
        JsonNode dataNode = results.get("Data");
        if (dataNode != null) {
          // ITA API returns a single data object, not an array
          ItaData ita = new ItaData();
          ita.indicator = dataNode.get("Indicator").asText();
          ita.areaOrCountry = dataNode.get("AreaOrCountry").asText();
          ita.frequency = dataNode.get("Frequency").asText();
          ita.year = Integer.parseInt(dataNode.get("Year").asText());
          ita.timeSeriesId = dataNode.get("TimeSeriesId").asText();
          ita.timeSeriesDescription = dataNode.get("TimeSeriesDescription").asText();
          ita.units = "USD Millions";

          // Parse value, handling special cases
          String dataValue = dataNode.get("DataValue").asText();
          if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
            try {
              ita.value = Double.parseDouble(dataValue.replace(",", ""));
            } catch (NumberFormatException e) {
              LOGGER.debug("Failed to parse ITA data value: {}", dataValue);
              continue; // Skip invalid values
            }
          } else {
            LOGGER.debug("Skipping ITA record with invalid data value: {}", dataValue);
            continue;
          }

          // Set indicator description
          ita.indicatorDescription = getItaIndicatorDescription(ita.indicator);

          itaRecords.add(ita);
        }
      }
    }

    // Save as JSON to cache using StorageProvider
    // Use type=indicators pattern to match where converters expect files
    String relativePath = "type=indicators/year=" + year + "/ita_data.json";
    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> itaList = new ArrayList<>();

    for (ItaData ita : itaRecords) {
      Map<String, Object> itaMap = new HashMap<>();
      itaMap.put("indicator", ita.indicator);
      itaMap.put("indicator_description", ita.indicatorDescription);
      itaMap.put("area_or_country", ita.areaOrCountry);
      itaMap.put("frequency", ita.frequency);
      itaMap.put("year", ita.year);
      itaMap.put("value", ita.value);
      itaMap.put("units", ita.units);
      itaMap.put("time_series_id", ita.timeSeriesId);
      itaMap.put("time_series_description", ita.timeSeriesDescription);
      itaList.add(itaMap);
    }

    data.put("ita_data", itaList);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);

    String jsonContent = MAPPER.writeValueAsString(data);
    // Resolve path against cacheDirectory (which includes source=econ) before writing
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, jsonContent.getBytes(StandardCharsets.UTF_8));

    LOGGER.debug("ITA data saved to: {} ({} records)", relativePath, itaRecords.size());

    // Mark as cached in manifest (use relative path)
    cacheManifest.markCached("ita_data", year, cacheParams, relativePath, 0L);
    cacheManifest.save(operatingDirectory);
  }

  /**
   * Downloads comprehensive International Transactions Accounts (ITA) data.
   * Provides detailed trade balances, current account, and capital flows.
   * @return The storage path (local or S3) where the data was saved
   */
  public String downloadItaData(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }

    // Build RELATIVE path (StorageProvider will add base path)
    String relativePath =
        String.format("type=ita_data/year_range=%d_%d/ita_data.parquet", startYear, endYear);

    List<ItaData> itaRecords = new ArrayList<>();

    // Build year list for API request
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    String yearParam = String.join(",", years);

    // Download key ITA indicators
    String[] indicators = {
        ItaIndicators.BALANCE_GOODS,
        ItaIndicators.BALANCE_SERVICES,
        ItaIndicators.BALANCE_GOODS_SERVICES,
        ItaIndicators.BALANCE_CURRENT_ACCOUNT,
        ItaIndicators.BALANCE_CAPITAL_ACCOUNT,
        ItaIndicators.BALANCE_PRIMARY_INCOME,
        ItaIndicators.BALANCE_SECONDARY_INCOME
    };

    for (String indicator : indicators) {
      String params =
          String.format("UserID=%s&method=GetData&datasetname=%s&Indicator=%s&AreaOrCountry=AllCountries&Frequency=A&Year=%s&ResultFormat=JSON", apiKey, Datasets.ITA, indicator, yearParam);

      String url = BEA_API_BASE + "?" + params;

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        LOGGER.warn("ITA API request failed for indicator {} with status: {}", indicator, response.statusCode());
        continue;
      }

      JsonNode root = MAPPER.readTree(response.body());
      JsonNode results = root.get("BEAAPI").get("Results");

      if (results != null && results.has("Data")) {
        JsonNode dataArray = results.get("Data");
        if (dataArray != null && dataArray.isArray()) {
          for (JsonNode record : dataArray) {
            ItaData ita = new ItaData();
            ita.indicator = record.get("Indicator").asText();
            ita.areaOrCountry = record.get("AreaOrCountry").asText();
            ita.frequency = record.get("Frequency").asText();
            ita.year = Integer.parseInt(record.get("Year").asText());
            ita.timeSeriesId = record.get("TimeSeriesId").asText();
            ita.timeSeriesDescription = record.get("TimeSeriesDescription").asText();
            ita.units = "USD Millions";

            // Parse value, handling special cases
            String dataValue = record.get("DataValue").asText();
            if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue)) {
              try {
                ita.value = Double.parseDouble(dataValue.replace(",", ""));
              } catch (NumberFormatException e) {
                continue; // Skip invalid values
              }
            } else {
              continue;
            }

            // Set indicator description
            ita.indicatorDescription = getItaIndicatorDescription(ita.indicator);

            itaRecords.add(ita);
          }
        }
      }
    }

    // Convert to Parquet
    writeItaDataParquet(itaRecords, relativePath);

    LOGGER.debug("ITA data saved to: {} ({} records)", relativePath, itaRecords.size());
    return relativePath;
  }

  /**
   * Maps ITA indicator codes to human-readable descriptions.
   */
  private String getItaIndicatorDescription(String indicator) {
    switch (indicator) {
      case "BalGds": return "Balance on goods";
      case "BalServ": return "Balance on services";
      case "BalGdsServ": return "Balance on goods and services";
      case "BalCurrAcct": return "Balance on current account";
      case "BalCapAcct": return "Balance on capital account";
      case "BalPrimInc": return "Balance on primary income";
      case "BalSecInc": return "Balance on secondary income";
      default: return "Unknown indicator";
    }
  }

  private void writeItaDataParquet(List<ItaData> itaRecords, String outputPath) throws IOException {
    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (ItaData ita : itaRecords) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("indicator", ita.indicator);
      record.put("indicator_description", ita.indicatorDescription);
      record.put("area_or_country", ita.areaOrCountry);
      record.put("frequency", ita.frequency);
      // year comes from partition key (year=*/), not parquet file
      record.put("value", ita.value);
      record.put("units", ita.units);
      record.put("time_series_id", ita.timeSeriesId);
      record.put("time_series_description", ita.timeSeriesDescription);
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("ita_data");
    storageProvider.writeAvroParquet(outputPath, columns, dataRecords, "ItaData", "ItaData");
    LOGGER.debug("ITA data Parquet written: {} ({} records)", outputPath, itaRecords.size());
  }

  /**
   * Downloads GDP by Industry data using default date range.
   */
  public String downloadIndustryGdp() throws IOException, InterruptedException {
    return downloadIndustryGdp(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads industry GDP data for a single year.
   */
  public void downloadIndustryGdpForYear(int year) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }

    // Check if parquet file already exists (skip download if already converted)
    String industryGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/industry_gdp.parquet");
    if (storageProvider.exists(industryGdpParquetPath)) {
      LOGGER.debug("Found existing industry GDP parquet for year {} - skipping download", year);
      return;
    }

    // Check cache first
    Map<String, String> cacheParams = new HashMap<>();
    // Don't include redundant params - year is already a parameter, type is the dataType

    if (cacheManifest.isCached("industry_gdp", year, cacheParams)) {
      LOGGER.debug("Found cached industry GDP data for year {} - skipping download", year);
      return;
    }

    // Check if JSON file exists using StorageProvider and update manifest
    // Note: Industry GDP is mostly annual, but manufacturing can be quarterly (TODO: detect from data)
    // Use type=indicators pattern to match where converters expect files
    String relativePath = "type=indicators/year=" + year + "/industry_gdp.json";
    try {
      if (cacheStorageProvider.exists(relativePath)) {
        LOGGER.debug("Found existing industry GDP JSON file for year {} - updating manifest", year);
        cacheManifest.markCached("industry_gdp", year, cacheParams, relativePath, 0L);
        cacheManifest.save(operatingDirectory);
        return;
      }
    } catch (Exception e) {
      LOGGER.debug("Could not check if file exists: {}", e.getMessage());
    }

    // Directory creation handled automatically by StorageProvider when writing files

    LOGGER.debug("Downloading BEA GDP by Industry data for year {}", year);

    List<IndustryGdpData> industryData = new ArrayList<>();

    // Key industries to download (NAICS codes) - limited for single year
    String[] keyIndustries = {
        "31G",     // Manufacturing
        "52",      // Finance and insurance
        "53",      // Real estate and rental and leasing
        "54",      // Professional, scientific, and technical services
        "GSLG",    // Government
    };

    // Download annual data for Table 1 (Value Added by Industry)
    for (String industry : keyIndustries) {
      String params =
          String.format("UserID=%s&method=GetData&datasetname=%s&TableID=1&Frequency=A&Year=%d&Industry=%s&ResultFormat=JSON", apiKey, Datasets.GDP_BY_INDUSTRY, year, industry);

      String url = BEA_API_BASE + "?" + params;

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      try {
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
          LOGGER.warn("GDP by Industry API request failed for industry {} year {} with status: {}", industry, year, response.statusCode());
          continue;
        }

        JsonNode root = MAPPER.readTree(response.body());

        // The GDP by Industry API returns data in a different structure
        JsonNode results = root.get("BEAAPI").get("Results");
        if (results != null && results.isArray() && results.size() > 0) {
          JsonNode dataNode = results.get(0);
          if (dataNode.has("Data")) {
            JsonNode dataArray = dataNode.get("Data");
            if (dataArray != null && dataArray.isArray()) {
              for (JsonNode record : dataArray) {
                IndustryGdpData gdp = new IndustryGdpData();
                gdp.tableId = record.get("TableID").asInt();
                gdp.frequency = record.get("Frequency").asText();
                gdp.year = Integer.parseInt(record.get("Year").asText());
                gdp.quarter = record.get("Quarter").asText();
                gdp.industryCode = record.get("Industry").asText();
                gdp.industryDescription = record.get("IndustrYDescription").asText();
                gdp.units = "Billions of dollars";

                // Parse value, handling special cases
                String dataValue = record.get("DataValue").asText();
                if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue) && !dataValue.equals("...")) {
                  try {
                    gdp.value = Double.parseDouble(dataValue.replace(",", ""));
                  } catch (NumberFormatException e) {
                    continue; // Skip invalid values
                  }
                } else {
                  continue;
                }

                if (record.has("NoteRef")) {
                  gdp.noteRef = record.get("NoteRef").asText();
                }

                industryData.add(gdp);
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Error processing industry {} for year {}: {}", industry, year, e.getMessage());
      }
    }

    // Save as JSON to cache using StorageProvider
    // relativePath already declared at line 1491
    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> gdpList = new ArrayList<>();

    for (IndustryGdpData gdp : industryData) {
      Map<String, Object> gdpMap = new HashMap<>();
      gdpMap.put("table_id", gdp.tableId);
      gdpMap.put("frequency", gdp.frequency);
      gdpMap.put("year", gdp.year);
      gdpMap.put("quarter", gdp.quarter);
      gdpMap.put("industry_code", gdp.industryCode);
      gdpMap.put("industry_description", gdp.industryDescription);
      gdpMap.put("value", gdp.value);
      gdpMap.put("units", gdp.units);
      gdpMap.put("note_ref", gdp.noteRef);
      gdpList.add(gdpMap);
    }

    data.put("industry_gdp", gdpList);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);

    String jsonContent = MAPPER.writeValueAsString(data);
    // Resolve path against cacheDirectory (which includes source=econ) before writing
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, jsonContent.getBytes(StandardCharsets.UTF_8));

    LOGGER.debug("Industry GDP data saved to: {} ({} records)", relativePath, industryData.size());

    // Mark as cached in manifest (use relative path)
    cacheManifest.markCached("industry_gdp", year, cacheParams, relativePath, 0L);
    cacheManifest.save(operatingDirectory);
  }

  /**
   * Downloads GDP by Industry data showing value added by NAICS industry classification.
   * Provides quarterly and annual data for all industries including manufacturing,
   * services, finance, technology, and government sectors.
   * @return The storage path (local or S3) where the data was saved
   */
  public String downloadIndustryGdp(int startYear, int endYear) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }

    // Build RELATIVE path (StorageProvider will add base path)
    String relativePath =
        String.format("type=industry_gdp/year_range=%d_%d/industry_gdp.parquet", startYear, endYear);

    List<IndustryGdpData> industryData = new ArrayList<>();

    // Build year list for API request
    List<String> years = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(String.valueOf(year));
    }
    String yearParam = String.join(",", years);

    // Key industries to download (NAICS codes)
    String[] keyIndustries = {
        "11",      // Agriculture, forestry, fishing, and hunting
        "21",      // Mining
        "22",      // Utilities
        "23",      // Construction
        "31G",     // Manufacturing
        "42",      // Wholesale trade
        "44RT",    // Retail trade
        "48TW",    // Transportation and warehousing
        "51",      // Information
        "52",      // Finance and insurance
        "53",      // Real estate and rental and leasing
        "54",      // Professional, scientific, and technical services
        "55",      // Management of companies and enterprises
        "56",      // Administrative and waste management services
        "61",      // Educational services
        "62",      // Health care and social assistance
        "71",      // Arts, entertainment, and recreation
        "72",      // Accommodation and food services
        "81",      // Other services
        "GSLG",    // Government
    };

    // Download annual data for Table 1 (Value Added by Industry)
    for (String industry : keyIndustries) {
      String params =
          String.format("UserID=%s&method=GetData&datasetname=%s&TableID=1&Frequency=A&Year=%s&Industry=%s&ResultFormat=JSON", apiKey, Datasets.GDP_BY_INDUSTRY, yearParam, industry);

      String url = BEA_API_BASE + "?" + params;

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      try {
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
          LOGGER.warn("GDP by Industry API request failed for industry {} with status: {}", industry, response.statusCode());
          continue;
        }

        JsonNode root = MAPPER.readTree(response.body());

        // The GDP by Industry API returns data in a different structure
        JsonNode results = root.get("BEAAPI").get("Results");
        if (results != null && results.isArray() && results.size() > 0) {
          JsonNode dataNode = results.get(0);
          if (dataNode.has("Data")) {
            JsonNode dataArray = dataNode.get("Data");
            if (dataArray != null && dataArray.isArray()) {
              for (JsonNode record : dataArray) {
                IndustryGdpData gdp = new IndustryGdpData();
                gdp.tableId = record.get("TableID").asInt();
                gdp.frequency = record.get("Frequency").asText();
                gdp.year = Integer.parseInt(record.get("Year").asText());
                gdp.quarter = record.get("Quarter").asText();
                gdp.industryCode = record.get("Industry").asText();
                gdp.industryDescription = record.get("IndustrYDescription").asText();
                gdp.units = "Billions of dollars";

                // Parse value, handling special cases
                String dataValue = record.get("DataValue").asText();
                if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue) && !dataValue.equals("...")) {
                  try {
                    gdp.value = Double.parseDouble(dataValue.replace(",", ""));
                  } catch (NumberFormatException e) {
                    continue; // Skip invalid values
                  }
                } else {
                  continue;
                }

                if (record.has("NoteRef")) {
                  gdp.noteRef = record.get("NoteRef").asText();
                }

                industryData.add(gdp);
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Error processing industry {}: {}", industry, e.getMessage());
      }
    }

    // Also download quarterly data for recent years (last 2 years only for size)
    int quarterlyStartYear = Math.max(startYear, endYear - 1);
    for (int year = quarterlyStartYear; year <= endYear; year++) {
      for (String quarter : new String[]{"Q1", "Q2", "Q3", "Q4"}) {
        // Download quarterly data for manufacturing sector as example
        String params =
            String.format("UserID=%s&method=GetData&datasetname=%s&TableID=1&Frequency=Q&Year=%d&Quarter=%s&Industry=31G&ResultFormat=JSON", apiKey, Datasets.GDP_BY_INDUSTRY, year, quarter);

        String url = BEA_API_BASE + "?" + params;

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(30))
            .build();

        try {
          HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

          if (response.statusCode() == 200) {
            JsonNode root = MAPPER.readTree(response.body());
            JsonNode results = root.get("BEAAPI").get("Results");
            if (results != null && results.isArray() && results.size() > 0) {
              JsonNode dataNode = results.get(0);
              if (dataNode.has("Data")) {
                JsonNode dataArray = dataNode.get("Data");
                if (dataArray != null && dataArray.isArray()) {
                  for (JsonNode record : dataArray) {
                    IndustryGdpData gdp = new IndustryGdpData();
                    gdp.tableId = record.get("TableID").asInt();
                    gdp.frequency = record.get("Frequency").asText();
                    gdp.year = year;
                    gdp.quarter = quarter;
                    gdp.industryCode = record.get("Industry").asText();
                    gdp.industryDescription = record.get("IndustrYDescription").asText();
                    gdp.units = "Billions of dollars";

                    String dataValue = record.get("DataValue").asText();
                    if (!"NoteRef".equals(dataValue) && !dataValue.isEmpty() && !"(NA)".equals(dataValue) && !dataValue.equals("...")) {
                      try {
                        gdp.value = Double.parseDouble(dataValue.replace(",", ""));
                      } catch (NumberFormatException e) {
                        continue;
                      }
                    } else {
                      continue;
                    }

                    industryData.add(gdp);
                  }
                }
              }
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Quarterly data not available for {} {}", year, quarter);
        }
      }
    }

    // Convert to Parquet
    writeIndustryGdpParquet(industryData, relativePath);

    LOGGER.debug("Industry GDP data saved to: {} ({} records)", relativePath, industryData.size());
    return relativePath;
  }

  private void writeIndustryGdpParquet(List<IndustryGdpData> industryData, String outputPath) throws IOException {
    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (IndustryGdpData gdp : industryData) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("table_id", String.valueOf(gdp.tableId));
      record.put("frequency", gdp.frequency);
      // year comes from partition key (year=*/), not parquet file
      record.put("quarter", gdp.quarter);
      record.put("industry_code", gdp.industryCode);
      record.put("industry_description", gdp.industryDescription);
      record.put("value", gdp.value);
      record.put("units", gdp.units);
      record.put("note_ref", gdp.noteRef);
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("industry_gdp");
    storageProvider.writeAvroParquet(outputPath, columns, dataRecords, "IndustryGdpData", "IndustryGdpData");
    LOGGER.debug("Industry GDP Parquet written: {} ({} records)", outputPath, industryData.size());
  }

  private void writeGdpComponentsParquet(List<GdpComponent> components, String outputPath) throws IOException {
    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (GdpComponent component : components) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("table_id", component.tableId);
      record.put("line_number", component.lineNumber);
      record.put("line_description", component.lineDescription);
      record.put("series_code", component.seriesCode);
      // year comes from partition key (year=*/), not parquet file
      record.put("value", component.value);
      record.put("units", component.units);
      record.put("frequency", component.frequency);
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("gdp_components");
    storageProvider.writeAvroParquet(outputPath, columns, dataRecords, "GdpComponent", "GdpComponent");
    LOGGER.debug("GDP Components Parquet written: {} ({} records)", outputPath, components.size());
  }

  public void writeRegionalIncomeParquet(List<RegionalIncome> incomeData, String targetFilePath) throws IOException {
    // IMPORTANT: Do not include 'year' in the data records - it's a partition key derived from directory structure
    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (RegionalIncome income : incomeData) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("geo_fips", income.geoFips);
      record.put("geo_name", income.geoName);
      record.put("metric", income.metric);
      record.put("line_code", income.lineCode);
      record.put("line_description", income.lineDescription);
      // Don't put year - it's derived from the partition directory
      record.put("value", income.value);
      record.put("units", income.units);
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("regional_income");
    storageProvider.writeAvroParquet(targetFilePath, columns, dataRecords, "RegionalIncome", "RegionalIncome");
  }

  // Data classes
  private static class GdpComponent {
    String tableId;
    int lineNumber;
    String lineDescription;
    String seriesCode;
    int year;
    double value;
    String units;
    String frequency;
  }

  public static class TradeStatistic {
    String tableId;
    int lineNumber;
    String lineDescription;
    String seriesCode;
    int year;
    double value;
    String units;
    String frequency;
    String tradeType;  // "Exports" or "Imports"
    String category;   // Parsed from lineDescription
    double tradeBalance; // Calculated for matching import/export pairs
  }

  public static class RegionalIncome {
    String geoFips;
    String geoName;
    String metric;
    String lineCode;
    String lineDescription;
    int year;
    double value;
    String units;
  }

  private static class ItaData {
    String indicator;
    String indicatorDescription;
    String areaOrCountry;
    String frequency;
    int year;
    double value;
    String units;
    String timeSeriesId;
    String timeSeriesDescription;
  }

  private static class GdpByIndustryData {
    String tableId;
    String industry;
    String industryDescription;
    int year;
    String quarter;
    double value;
    String units;
    String metric;
    String frequency;
  }

  private static class IndustryGdpData {
    int tableId;
    String frequency;
    int year;
    String quarter;
    String industryCode;
    String industryDescription;
    double value;
    String units;
    String noteRef;
  }

  public static class StateGdp {
    String geoFips;
    String geoName;
    String lineCode;
    String lineDescription;
    int year;
    double value;
    String units;
  }

  /**
   * Creates an empty GDP components file for years where data is not available.
   */
  private void createEmptyGdpComponentsFile(int year, String reason) throws IOException {
    // Directory creation handled automatically by StorageProvider when writing files

    String relativePath = buildPartitionPath("gdp_components", DataFrequency.QUARTERLY, year) + "/gdp_components.json";
    Map<String, Object> data = new HashMap<>();
    data.put("components", new ArrayList<>());
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);
    data.put("no_data_reason", reason);

    String jsonContent = MAPPER.writeValueAsString(data);
    // Resolve path against cacheDirectory (which includes source=econ) before writing
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, jsonContent.getBytes(StandardCharsets.UTF_8));

    LOGGER.debug("Created empty GDP components file for year {}: {}", year, relativePath);
  }

  /**
   * Converts regional income JSON to Parquet format.
   */
  public void convertRegionalIncomeToParquet(String sourceDirPath, String targetFilePath) throws IOException {
    LOGGER.debug("Converting regional income data from {} to parquet: {}", sourceDirPath, targetFilePath);


    List<RegionalIncome> incomeData = new ArrayList<>();

    // Read regional income JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "regional_income.json");
    try {
      if (cacheStorageProvider.exists(jsonFilePath)) {
        try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
             InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
          JsonNode root = MAPPER.readTree(reader);
          JsonNode incomeArray = root.get("regional_income");

          if (incomeArray != null && incomeArray.isArray()) {
            for (JsonNode inc : incomeArray) {
              try {
                RegionalIncome income = new RegionalIncome();
                income.geoFips = inc.get("geo_fips").asText();
                income.geoName = inc.get("geo_name").asText();
                income.metric = inc.get("metric").asText();
                income.lineCode = inc.get("line_code").asText();
                income.lineDescription = inc.get("line_description").asText();
                income.year = inc.get("year").asInt();
                income.value = inc.get("value").asDouble();
                income.units = inc.get("units").asText();

                incomeData.add(income);
              } catch (Exception e) {
                LOGGER.warn("Failed to parse regional income record: {}", e.getMessage());
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to process regional income JSON file {}: {}", jsonFilePath, e.getMessage());
    }

    if (!incomeData.isEmpty()) {
      // Write parquet file
      writeRegionalIncomeParquet(incomeData, targetFilePath);
      LOGGER.debug("Converted regional income data to parquet: {} ({} records)", targetFilePath, incomeData.size());
    } else {
      LOGGER.warn("No regional income data found in {}", sourceDirPath);
    }
  }

  /**
   * Downloads state GDP data for a single year.
   */
  public void downloadStateGdpForYear(int year) throws IOException, InterruptedException {
    if (apiKey == null || apiKey.isEmpty()) {
      LOGGER.error("BEA API key is missing - cannot download state GDP data");
      throw new IllegalStateException("BEA API key is required. Set BEA_API_KEY environment variable.");
    }

    // Check cache first
    Map<String, String> cacheParams = new HashMap<>();
    // Don't include redundant params - year is already a parameter, type is the dataType

    if (cacheManifest.isCached("state_gdp", year, cacheParams)) {
      LOGGER.debug("Found cached state GDP data for year {} - skipping download", year);
      return;
    }

    // Check if data already exists using StorageProvider (Annual frequency)
    String relativePath = buildPartitionPath("state_gdp", DataFrequency.ANNUAL, year) + "/state_gdp.json";
    try {
      if (cacheStorageProvider.exists(relativePath)) {
        LOGGER.debug("Found existing state GDP file for year {} - updating manifest", year);
        cacheManifest.markCached("state_gdp", year, cacheParams, relativePath, 0L);
        cacheManifest.save(operatingDirectory);
        return;
      }
    } catch (Exception e) {
      LOGGER.debug("Could not check if file exists: {}", e.getMessage());
    }

    // Directory creation handled automatically by StorageProvider when writing files

    LOGGER.debug("Downloading BEA state GDP data for year {}", year);

    List<StateGdp> gdpData = new ArrayList<>();

    // Download state GDP data (LineCode=1 for total GDP, LineCode=2 for per capita)
    String[] lineCodes = {"1", "2"};
    String[] descriptions = {"All industry total", "Per capita real GDP"};

    for (int i = 0; i < lineCodes.length; i++) {
      String lineCode = lineCodes[i];
      String description = descriptions[i];

      String params =
          String.format("UserID=%s&method=GetData&datasetname=%s&TableName=SAGDP1&LineCode=%s&GeoFips=STATE&Year=%d&ResultFormat=JSON", apiKey, Datasets.REGIONAL, lineCode, year);

      String url = BEA_API_BASE + "?" + params;

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .GET()
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        LOGGER.warn("BEA state GDP API request failed for year {} LineCode {} with status: {}", year, lineCode, response.statusCode());
        continue;
      }

      JsonNode root = MAPPER.readTree(response.body());
      JsonNode beaApi = root.get("BEAAPI");
      if (beaApi != null) {
        JsonNode results = beaApi.get("Results");
        if (results != null) {
          JsonNode dataArray = results.get("Data");
          if (dataArray != null && dataArray.isArray()) {
            for (JsonNode record : dataArray) {
              try {
                StateGdp gdp = new StateGdp();

                JsonNode geoFipsNode = record.get("GeoFips");
                JsonNode geoNameNode = record.get("GeoName");
                JsonNode lineCodeNode = record.get("Code");
                JsonNode dataValueNode = record.get("DataValue");
                JsonNode unitMultNode = record.get("UNIT_MULT");

                if (geoFipsNode == null || geoNameNode == null || dataValueNode == null) {
                  continue;
                }

                gdp.geoFips = geoFipsNode.asText();
                gdp.geoName = geoNameNode.asText();
                gdp.lineCode = lineCodeNode != null ? lineCodeNode.asText() : lineCode;
                gdp.lineDescription = description;
                gdp.year = year;

                String valueStr = dataValueNode.asText();
                if ("NaN".equals(valueStr) || valueStr.isEmpty()) {
                  gdp.value = 0.0;
                } else {
                  gdp.value = Double.parseDouble(valueStr.replaceAll(",", ""));
                }

                gdp.units = lineCode.equals("2") ? "Dollars" :
                    (unitMultNode != null ? unitMultNode.asText() : "Millions of Dollars");

                gdpData.add(gdp);
              } catch (Exception e) {
                LOGGER.debug("Failed to parse state GDP record: {}", e.getMessage());
              }
            }
          }
        }
      }
    }

    // Save as JSON to cache using StorageProvider
    LOGGER.debug("Preparing to save {} state GDP records to {}", gdpData.size(), relativePath);

    Map<String, Object> data = new HashMap<>();
    List<Map<String, Object>> gdpList = new ArrayList<>();

    for (StateGdp gdp : gdpData) {
      Map<String, Object> gdpMap = new HashMap<>();
      gdpMap.put("geo_fips", gdp.geoFips);
      gdpMap.put("geo_name", gdp.geoName);
      gdpMap.put("line_code", gdp.lineCode);
      gdpMap.put("line_description", gdp.lineDescription);
      gdpMap.put("year", gdp.year);
      gdpMap.put("value", gdp.value);
      gdpMap.put("units", gdp.units);
      gdpList.add(gdpMap);
    }

    data.put("state_gdp", gdpList);
    data.put("download_date", LocalDate.now().toString());
    data.put("year", year);

    String jsonContent = MAPPER.writeValueAsString(data);
    // Resolve path against cacheDirectory (which includes source=econ) before writing
    String filePath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
    cacheStorageProvider.writeFile(filePath, jsonContent.getBytes(StandardCharsets.UTF_8));

    LOGGER.debug("State GDP data saved to: {} ({} records)", relativePath, gdpData.size());

    // Mark as cached in manifest (use relative path)
    cacheManifest.markCached("state_gdp", year, cacheParams, relativePath, 0L);
    cacheManifest.save(operatingDirectory);
  }

  public void writeStateGdpParquet(List<StateGdp> gdpData, String targetFilePath) throws IOException {
    // IMPORTANT: Do not include 'year' in the data records - it's a partition key derived from directory structure
    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (StateGdp gdp : gdpData) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("geo_fips", gdp.geoFips);
      record.put("geo_name", gdp.geoName);
      record.put("metric", gdp.lineDescription != null ? gdp.lineDescription : "GDP");
      record.put("value", gdp.value);
      record.put("units", gdp.units);
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("state_gdp");
    storageProvider.writeAvroParquet(targetFilePath, columns, dataRecords, "StateGdp", "StateGdp");
  }

  /**
   * Converts state GDP JSON to Parquet format.
   */
  public void convertStateGdpToParquet(String jsonFilePath, String targetFilePath) throws IOException {
    LOGGER.debug("Converting state GDP data from {} to parquet: {}", jsonFilePath, targetFilePath);


    List<StateGdp> gdpData = new ArrayList<>();

    // Read state GDP JSON file from cache using cacheStorageProvider
//    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "state_gdp.json");
    if (cacheStorageProvider.exists(jsonFilePath)) {
      try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
           InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(reader);
        JsonNode gdpArray = root.get("state_gdp");

        if (gdpArray != null && gdpArray.isArray()) {
          for (JsonNode gdpNode : gdpArray) {
            try {
              StateGdp gdp = new StateGdp();
              gdp.geoFips = gdpNode.get("geo_fips").asText();
              gdp.geoName = gdpNode.get("geo_name").asText();
              gdp.lineCode = gdpNode.get("line_code").asText();
              gdp.lineDescription = gdpNode.get("line_description").asText();
              gdp.year = gdpNode.get("year").asInt();
              gdp.value = gdpNode.get("value").asDouble();
              gdp.units = gdpNode.get("units").asText();

              gdpData.add(gdp);
            } catch (Exception e) {
              LOGGER.warn("Failed to parse state GDP record: {}", e.getMessage());
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to process state GDP JSON file {}: {}", jsonFilePath, e.getMessage());
      }
    }

    if (!gdpData.isEmpty()) {
      // Write a parquet file
      writeStateGdpParquet(gdpData, targetFilePath);
      LOGGER.debug("Converted state GDP data to parquet: {} ({} records)", targetFilePath, gdpData.size());

      // Clean up macOS metadata files that can interfere with DuckDB
      try {
        int lastSeparator = targetFilePath.lastIndexOf('/');
        if (lastSeparator > 0) {
          String parentDir = targetFilePath.substring(0, lastSeparator);
          storageProvider.cleanupMacosMetadata(parentDir);
        }
      } catch (IOException e) {
        LOGGER.warn("Failed to clean up metadata files for {}: {}", targetFilePath, e.getMessage());
      }
    } else {
      LOGGER.warn("No state GDP data found in {}", jsonFilePath);
    }
  }

  /**
   * Converts cached BEA GDP components data to Parquet format.
   * This method is called by EconSchemaFactory after downloading data.
   *
   * @param sourceDirPath Directory containing cached BEA JSON data
   * @param targetFilePath Target parquet file path to create
   */
  public void convertToParquet(String sourceDirPath, String targetFilePath) throws IOException {
    // Extract year and data type
    LOGGER.debug("Converting BEA data from {} to parquet: {}", sourceDirPath, targetFilePath);

    List<Map<String, Object>> components = new ArrayList<>();

    // Read GDP components JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "gdp_components.json");
    try {
      if (cacheStorageProvider.exists(jsonFilePath)) {
        try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
             java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
          JsonNode root = MAPPER.readTree(reader);
          JsonNode componentsArray = root.get("components");

          if (componentsArray != null && componentsArray.isArray()) {
            for (JsonNode comp : componentsArray) {
              Map<String, Object> component = new HashMap<>();
              component.put("table_id", comp.get("table_id").asText());
              component.put("line_number", comp.get("line_number").asInt());
              component.put("line_description", comp.get("line_description").asText());
              component.put("series_code", comp.get("series_code").asText());
              component.put("year", comp.get("year").asInt());
              component.put("value", comp.get("value").asDouble());
              component.put("units", comp.get("units").asText());
              component.put("frequency", comp.get("frequency").asText());

              components.add(component);
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to process BEA JSON file {}: {}", jsonFilePath, e.getMessage());
    }

    // Write parquet file
    writeGdpComponentsMapParquet(components, targetFilePath);

    LOGGER.debug("Converted BEA data to parquet: {} ({} components)", targetFilePath, components.size());
  }


  private void writeGdpComponentsMapParquet(List<Map<String, Object>> components, String targetFilePath)
      throws IOException {
    // Data is already in Map format, just pass through
    // (keep all transformation logic - none needed here)

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("gdp_components");
    storageProvider.writeAvroParquet(targetFilePath, columns, components, "GdpComponent", "GdpComponent");
  }

  /**
   * Converts trade statistics JSON files to Parquet format.
   */
  public void convertTradeStatisticsToParquet(String sourceDirPath, String targetFilePath) throws IOException {
    LOGGER.debug("Converting trade statistics data from {} to parquet: {}", sourceDirPath, targetFilePath);


    // Read JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "trade_statistics.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("Trade statistics JSON file not found: {}", jsonFilePath);
      return;
    }

    String jsonContent;
    try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath)) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    List<Map<String, Object>> records = parseTradeStatisticsJson(jsonContent);

    if (records.isEmpty()) {
      LOGGER.warn("No trade statistics records found in {}", jsonFilePath);
      return;
    }

    writeTradeStatisticsMapParquet(records, targetFilePath);
  }

  private List<Map<String, Object>> parseTradeStatisticsJson(String jsonContent) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(jsonContent);

    List<Map<String, Object>> records = new ArrayList<>();

    // Check for simplified format (custom download format)
    JsonNode tradeStatsArray = rootNode.path("trade_statistics");
    if (tradeStatsArray.isArray() && tradeStatsArray.size() > 0) {
      for (JsonNode item : tradeStatsArray) {
        Map<String, Object> record = new HashMap<>();
        record.put("table_id", item.path("table_id").asText());
        record.put("line_number", item.path("line_number").asInt());
        record.put("line_description", item.path("line_description").asText());
        record.put("series_code", item.path("series_code").asText());
        record.put("year", item.path("year").asInt());
        record.put("value", item.path("value").asDouble());
        record.put("units", item.path("units").asText("Billions of dollars"));
        record.put("frequency", item.path("frequency").asText("A"));
        record.put("trade_type", item.path("trade_type").asText());
        record.put("category", item.path("category").asText());
        record.put("trade_balance", item.path("trade_balance").asDouble(0.0));
        records.add(record);
      }
      return records;
    }

    // Fall back to BEA API format
    JsonNode results = rootNode.path("BEAAPI").path("Results");
    if (results.isArray() && results.size() > 0) {
      JsonNode data = results.get(0).path("Data");

      for (JsonNode item : data) {
        Map<String, Object> record = new HashMap<>();
        record.put("table_id", item.path("TableID").asText());
        record.put("line_number", item.path("LineNumber").asText());
        record.put("line_description", item.path("LineDescription").asText());
        record.put("series_code", item.path("SeriesCode").asText());
        record.put("year", item.path("TimePeriod").asInt());
        record.put("value", item.path("DataValue").asDouble());
        record.put("units", item.path("UNIT_MULT").asText("Millions of Dollars"));
        record.put("frequency", "A");
        record.put("trade_type", parseTradeType(item.path("LineDescription").asText()));
        record.put("category", parseTradeCategory(item.path("LineDescription").asText()));
        record.put("trade_balance", 0.0);
        records.add(record);
      }
    }

    return records;
  }

  private void writeTradeStatisticsMapParquet(List<Map<String, Object>> records, String targetFilePath) throws IOException {
    // Data is already in Map format, just pass through
    // (keep all transformation logic - none needed here)

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("trade_statistics");
    storageProvider.writeAvroParquet(targetFilePath, columns, records, "TradeStatistics", "TradeStatistics");
  }

  /**
   * Detects frequency codes present in trade_statistics.json under the given year folder.
   */
  public java.util.Set<String> detectTradeFrequencies(String sourceDirPath) throws IOException {
    java.util.Set<String> freqs = new java.util.LinkedHashSet<>();
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "trade_statistics.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      return freqs;
    }
    String jsonContent;
    try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath)) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    java.util.List<java.util.Map<String, Object>> records = parseTradeStatisticsJson(jsonContent);
    for (java.util.Map<String, Object> rec : records) {
      Object f = rec.get("frequency");
      if (f != null) {
        String v = f.toString().trim();
        if (!v.isEmpty()) {
          freqs.add(v);
        }
      }
    }
    return freqs;
  }

  /**
   * Converts trade_statistics to Parquet for a specific frequency bucket while excluding
   * the frequency column from rows (frequency is a partition key). Year stays in rows to
   * match current schema.
   */
  public void convertTradeStatisticsToParquetForFrequency(String sourceDirPath, String targetFilePath, String frequency) throws IOException {
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "trade_statistics.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("Trade statistics JSON file not found for frequency {}: {}", frequency, jsonFilePath);
      // Still create an empty Parquet to materialize the partition
      writeTradeStatisticsMapParquet(java.util.Collections.emptyList(), targetFilePath);
      return;
    }

    String jsonContent;
    try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath)) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    java.util.List<java.util.Map<String, Object>> all = parseTradeStatisticsJson(jsonContent);

    java.util.List<java.util.Map<String, Object>> filtered = new java.util.ArrayList<>();
    for (java.util.Map<String, Object> rec : all) {
      Object f = rec.get("frequency");
      String freq = f == null ? "" : f.toString();
      if (freq.equalsIgnoreCase(frequency)) {
        java.util.Map<String, Object> out = new java.util.HashMap<>();
        // Copy all known non-partition fields (keep 'year' per current schema)
        if (rec.containsKey("table_id")) out.put("table_id", rec.get("table_id"));
        if (rec.containsKey("line_number")) out.put("line_number", rec.get("line_number"));
        if (rec.containsKey("line_description")) out.put("line_description", rec.get("line_description"));
        if (rec.containsKey("series_code")) out.put("series_code", rec.get("series_code"));
        // drop 'year' (partition key)
        if (rec.containsKey("value")) out.put("value", rec.get("value"));
        if (rec.containsKey("units")) out.put("units", rec.get("units"));
        if (rec.containsKey("trade_type")) out.put("trade_type", rec.get("trade_type"));
        if (rec.containsKey("category")) out.put("category", rec.get("category"));
        if (rec.containsKey("trade_balance")) out.put("trade_balance", rec.get("trade_balance"));
        // drop 'frequency'
        filtered.add(out);
      }
    }

    LOGGER.info("Converting trade_statistics to parquet for frequency={} at {} ({} rows)", frequency, targetFilePath, filtered.size());
    writeTradeStatisticsMapParquet(filtered, targetFilePath);
  }

  /**
   * Converts ITA data JSON files to Parquet format.
   */
  public void convertItaDataToParquet(String sourceDirPath, String targetFilePath) throws IOException {
    LOGGER.debug("Converting ITA data from {} to parquet: {}", sourceDirPath, targetFilePath);

    // Backward-compat single-file conversion (legacy path without frequency partition)
    // Reads full JSON and writes as-is (including frequency). Retained for compatibility
    // but not used by the new frequency-partitioned conversion in EconSchemaFactory.

    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "ita_data.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("ITA data JSON file not found: {}", jsonFilePath);
      return;
    }

    String jsonContent;
    try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath)) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    List<Map<String, Object>> records = parseItaDataJson(jsonContent);

    if (records.isEmpty()) {
      LOGGER.warn("No ITA data records found in {}, creating empty Parquet file", jsonFilePath);
    }

    writeItaDataParquetFromMaps(records, targetFilePath);
  }

  /**
   * Converts ITA data to Parquet for a specific frequency bucket (A/Q/M) while
   * excluding partition columns (frequency, year) from Parquet rows.
   * Raw JSON remains under type=indicators/year=YYYY/ita_data.json.
   */
  /**
   * Detects the set of frequency codes present in the ITA JSON for a given year directory.
   * Raw JSON path: type=indicators/year=YYYY/ita_data.json
   */
  public java.util.Set<String> detectItaFrequencies(String sourceDirPath) throws IOException {
    java.util.Set<String> freqs = new java.util.LinkedHashSet<>();
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "ita_data.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      return freqs; // empty
    }
    String jsonContent;
    try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath)) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    List<Map<String, Object>> all = parseItaDataJson(jsonContent);
    for (Map<String, Object> rec : all) {
      Object f = rec.get("frequency");
      if (f != null) {
        String v = f.toString().trim();
        if (!v.isEmpty()) {
          freqs.add(v);
        }
      }
    }
    return freqs;
  }

  public void convertItaDataToParquetForFrequency(String sourceDirPath, String targetFilePath, String frequency) throws IOException {
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "ita_data.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("ITA data JSON file not found for frequency {}: {}", frequency, jsonFilePath);
      // Still create an empty Parquet to materialize the partition
      writeItaDataParquetFromMaps(java.util.Collections.emptyList(), targetFilePath);
      return;
    }

    String jsonContent;
    try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath)) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    List<Map<String, Object>> all = parseItaDataJson(jsonContent);

    // Filter by frequency (case-insensitive) and drop partition fields
    List<Map<String, Object>> filtered = new java.util.ArrayList<>();
    for (Map<String, Object> rec : all) {
      Object f = rec.get("frequency");
      String freq = f == null ? "" : f.toString();
      if (freq.equalsIgnoreCase(frequency)) {
        Map<String, Object> out = new java.util.HashMap<>();
        // Keep only non-partition columns
        if (rec.containsKey("indicator")) out.put("indicator", rec.get("indicator"));
        if (rec.containsKey("indicator_description")) out.put("indicator_description", rec.get("indicator_description"));
        if (rec.containsKey("area_or_country")) out.put("area_or_country", rec.get("area_or_country"));
        if (rec.containsKey("value")) out.put("value", rec.get("value"));
        if (rec.containsKey("units")) out.put("units", rec.get("units"));
        if (rec.containsKey("time_series_id")) out.put("time_series_id", rec.get("time_series_id"));
        if (rec.containsKey("time_series_description")) out.put("time_series_description", rec.get("time_series_description"));
        // intentionally drop 'frequency' and 'year' (partition keys)
        filtered.add(out);
      }
    }

    LOGGER.info("Converting ITA data to parquet for frequency={} at {} ({} rows)", frequency, targetFilePath, filtered.size());
    writeItaDataParquetFromMaps(filtered, targetFilePath);
  }

  private List<Map<String, Object>> parseItaDataJson(String jsonContent) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(jsonContent);

    List<Map<String, Object>> records = new ArrayList<>();

    // First, try the simplified format (what we actually have)
    JsonNode itaData = rootNode.path("ita_data");
    if (itaData.isArray()) {
      for (JsonNode item : itaData) {
        Map<String, Object> record = new HashMap<>();
        record.put("indicator", item.path("indicator").asText(""));
        record.put("indicator_description", item.path("indicator_description").asText(""));
        record.put("area_or_country", item.path("area_or_country").asText(""));
        record.put("frequency", item.path("frequency").asText("A"));
        record.put("year", item.path("year").asInt(0));
        record.put("value", item.path("value").asDouble(0.0));
        record.put("units", item.path("units").asText("USD Millions"));
        record.put("time_series_id", item.path("time_series_id").asText(""));
        record.put("time_series_description", item.path("time_series_description").asText(""));
        records.add(record);
      }
    } else {
      // Fall back to BEA API format if present
      JsonNode results = rootNode.path("BEAAPI").path("Results");
      if (results.isArray() && results.size() > 0) {
        JsonNode data = results.get(0).path("Data");

        for (JsonNode item : data) {
          Map<String, Object> record = new HashMap<>();
          record.put("table_id", item.path("TableID").asText());
          record.put("line_number", item.path("LineNumber").asText());
          record.put("line_description", item.path("LineDescription").asText());
          record.put("series_code", item.path("SeriesCode").asText());
          record.put("value", item.path("DataValue").asDouble());
          record.put("units", item.path("UNIT_MULT").asText("Millions of Dollars"));
          record.put("frequency", "A");
          records.add(record);
        }
      }
    }

    return records;
  }

  private void writeItaDataParquetFromMaps(List<Map<String, Object>> records, String outputFile) throws IOException {
    // Data is already in Map format, just pass through
    // (keep all transformation logic - none needed here)

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("ita_data");
    storageProvider.writeAvroParquet(outputFile, columns, records, "ItaData", "ItaData");
    LOGGER.debug("ITA data Parquet written: {} ({} records)", outputFile, records.size());
  }

  /**
   * Converts industry GDP JSON files to Parquet format.
   */
  public void convertIndustryGdpToParquet(String sourceDirPath, String targetFilePath) throws IOException {

    LOGGER.debug("Converting industry GDP data from {} to parquet: {}", sourceDirPath, targetFilePath);

    // Read JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "industry_gdp.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("Industry GDP JSON file not found: {}", jsonFilePath);
      return;
    }

    String jsonContent;
    try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath)) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    List<Map<String, Object>> records = parseIndustryGdpJson(jsonContent);

    if (records.isEmpty()) {
      LOGGER.warn("No industry GDP records found in {}", jsonFilePath);
      return;
    }

    writeIndustryGdpParquetFromMaps(records, targetFilePath);
  }

  private List<Map<String, Object>> parseIndustryGdpJson(String jsonContent) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = mapper.readTree(jsonContent);

    List<Map<String, Object>> records = new ArrayList<>();

    // Check for simplified format (custom download format)
    JsonNode industryGdpArray = rootNode.path("industry_gdp");
    if (industryGdpArray.isArray() && industryGdpArray.size() > 0) {
      for (JsonNode item : industryGdpArray) {
        Map<String, Object> record = new HashMap<>();
        record.put("table_id", item.path("table_id").asText());
        record.put("year", item.path("year").asInt());
        record.put("quarter", item.path("quarter").asText());
        record.put("industry_code", item.path("industry_code").asText());
        record.put("industry_description", item.path("industry_description").asText());
        record.put("value", item.path("value").asDouble());
        record.put("units", item.path("units").asText("Billions of dollars"));
        record.put("frequency", item.path("frequency").asText("A"));
        record.put("note_ref", item.path("note_ref").asText(""));
        records.add(record);
      }
      return records;
    }

    // Fall back to BEA API format
    JsonNode results = rootNode.path("BEAAPI").path("Results");
    if (results.isArray() && results.size() > 0) {
      JsonNode data = results.get(0).path("Data");

      for (JsonNode item : data) {
        Map<String, Object> record = new HashMap<>();
        record.put("table_id", item.path("TableID").asText());
        record.put("line_number", item.path("LineNumber").asText());
        record.put("line_description", item.path("LineDescription").asText());
        record.put("series_code", item.path("SeriesCode").asText());
        record.put("value", item.path("DataValue").asDouble());
        record.put("units", item.path("UNIT_MULT").asText("Millions of Dollars"));
        record.put("frequency", "A");
        record.put("industry", parseIndustryFromDescription(item.path("LineDescription").asText()));
        records.add(record);
      }
    }

    return records;
  }

  private void writeIndustryGdpParquetFromMaps(List<Map<String, Object>> records, String outputFile) throws IOException {
    // Data is already in Map format, just pass through
    // (keep all transformation logic - none needed here)

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("industry_gdp");
    storageProvider.writeAvroParquet(outputFile, columns, records, "IndustryGdp", "IndustryGdp");
  }

  /**
   * Detects frequency codes present in industry_gdp.json under the given year folder.
   */
  public java.util.Set<String> detectIndustryGdpFrequencies(String sourceDirPath) throws IOException {
    java.util.Set<String> freqs = new java.util.LinkedHashSet<>();
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "industry_gdp.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      return freqs;
    }
    String jsonContent;
    try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath)) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    java.util.List<java.util.Map<String, Object>> records = parseIndustryGdpJson(jsonContent);
    for (java.util.Map<String, Object> rec : records) {
      Object f = rec.get("frequency");
      if (f != null) {
        String v = f.toString().trim();
        if (!v.isEmpty()) {
          freqs.add(v);
        }
      }
    }
    return freqs;
  }

  /**
   * Converts industry_gdp to Parquet for a specific frequency bucket while excluding
   * the frequency and year columns from rows (both are partition keys).
   */
  public void convertIndustryGdpToParquetForFrequency(String sourceDirPath, String targetFilePath, String frequency) throws IOException {
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "industry_gdp.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("Industry GDP JSON file not found for frequency {}: {}", frequency, jsonFilePath);
      // Create empty parquet to materialize the partition
      writeIndustryGdpParquetFromMaps(java.util.Collections.emptyList(), targetFilePath);
      return;
    }

    String jsonContent;
    try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath)) {
      jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
    java.util.List<java.util.Map<String, Object>> all = parseIndustryGdpJson(jsonContent);

    java.util.List<java.util.Map<String, Object>> filtered = new java.util.ArrayList<>();
    for (java.util.Map<String, Object> rec : all) {
      Object f = rec.get("frequency");
      String freq = f == null ? "" : f.toString();
      if (freq.equalsIgnoreCase(frequency)) {
        java.util.Map<String, Object> out = new java.util.HashMap<>();
        // Copy non-partition fields expected by schema
        if (rec.containsKey("table_id")) out.put("table_id", rec.get("table_id"));
        if (rec.containsKey("quarter")) out.put("quarter", rec.get("quarter"));
        if (rec.containsKey("industry_code")) out.put("industry_code", rec.get("industry_code"));
        if (rec.containsKey("industry_description")) out.put("industry_description", rec.get("industry_description"));
        if (rec.containsKey("value")) out.put("value", rec.get("value"));
        if (rec.containsKey("units")) out.put("units", rec.get("units"));
        if (rec.containsKey("note_ref")) out.put("note_ref", rec.get("note_ref"));
        // drop 'frequency' and 'year'
        filtered.add(out);
      }
    }

    LOGGER.info("Converting industry_gdp to parquet for frequency={} at {} ({} rows)", frequency, targetFilePath, filtered.size());
    writeIndustryGdpParquetFromMaps(filtered, targetFilePath);
  }

  private String parseTradeType(String description) {
    String desc = description.toLowerCase();
    if (desc.contains("export")) {
      return "Exports";
    } else if (desc.contains("import")) {
      return "Imports";
    } else {
      return "Other";
    }
  }

  private String parseTradeCategory(String description) {
    String desc = description.toLowerCase();
    if (desc.contains("goods")) {
      return "Goods";
    } else if (desc.contains("services")) {
      return "Services";
    } else if (desc.contains("food")) {
      return "Food";
    } else {
      return "Other";
    }
  }

  private String parseIndustryFromDescription(String description) {
    String desc = description.toLowerCase();
    if (desc.contains("agriculture")) {
      return "Agriculture";
    } else if (desc.contains("mining")) {
      return "Mining";
    } else if (desc.contains("manufacturing")) {
      return "Manufacturing";
    } else if (desc.contains("construction")) {
      return "Construction";
    } else if (desc.contains("finance")) {
      return "Finance";
    } else if (desc.contains("retail")) {
      return "Retail";
    } else if (desc.contains("real estate")) {
      return "Real Estate";
    } else if (desc.contains("information")) {
      return "Information";
    } else {
      return "Other";
    }
  }

  /**
   * Convert GDP statistics data to Parquet format.
   */
  public void convertGdpStatisticsToParquet(String sourceDirPath, String targetFilePath) throws IOException {
    // Extract year and data type for cache check
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "gdp_components.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No gdp_components.json found in {}", sourceDirPath);
      return;
    }

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    Map<String, Object> data;
    try (InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      data = MAPPER.readValue(reader, Map.class);
    }
    // The field is "components" not "gdp_components"
    List<Map<String, Object>> components = (List<Map<String, Object>>) data.get("components");

    if (components != null && !components.isEmpty()) {
      // Extract GDP statistics from components
      Map<String, Double> gdpMetrics = new HashMap<>();
      for (Map<String, Object> component : components) {
        String lineDesc = (String) component.get("line_description");
        Double value = ((Number) component.get("value")).doubleValue();

        // Look for key GDP metrics
        if (lineDesc.contains("Gross domestic product")) {
          gdpMetrics.put("Nominal GDP", value);
        } else if (lineDesc.contains("Real gross domestic product")) {
          gdpMetrics.put("Real GDP", value);
        } else if (lineDesc.contains("Personal consumption")) {
          gdpMetrics.put("Personal Consumption", value);
        } else if (lineDesc.contains("Gross private domestic investment")) {
          gdpMetrics.put("Private Investment", value);
        } else if (lineDesc.contains("Government consumption")) {
          gdpMetrics.put("Government Spending", value);
        } else if (lineDesc.contains("Net exports")) {
          gdpMetrics.put("Net Exports", value);
        }
      }

      // Create GDP statistics records
      // Year is stored as Integer in the JSON
      int year = ((Number) components.get(0).get("year")).intValue();

      // Try to load previous year's data for calculating percent changes
      Map<String, Double> previousYearMetrics = new HashMap<>();
      // Build path to previous year's file by replacing year in path
      String previousYearPath = sourceDirPath.replaceAll("year=\\d+", "year=" + (year - 1)) + "/gdp_components.json";
      if (cacheStorageProvider.exists(previousYearPath)) {
        try (InputStream previousInputStream = cacheStorageProvider.openInputStream(previousYearPath);
             InputStreamReader previousReader = new InputStreamReader(previousInputStream, StandardCharsets.UTF_8)) {
          Map<String, Object> previousData = MAPPER.readValue(previousReader, Map.class);
          List<Map<String, Object>> previousComponents = (List<Map<String, Object>>) previousData.get("components");
          if (previousComponents != null && !previousComponents.isEmpty()) {
            for (Map<String, Object> component : previousComponents) {
              String lineDesc = (String) component.get("line_description");
              Double value = ((Number) component.get("value")).doubleValue();

              if (lineDesc.contains("Gross domestic product")) {
                previousYearMetrics.put("Nominal GDP", value);
              } else if (lineDesc.contains("Real gross domestic product")) {
                previousYearMetrics.put("Real GDP", value);
              } else if (lineDesc.contains("Personal consumption")) {
                previousYearMetrics.put("Personal Consumption", value);
              } else if (lineDesc.contains("Gross private domestic investment")) {
                previousYearMetrics.put("Private Investment", value);
              } else if (lineDesc.contains("Government consumption")) {
                previousYearMetrics.put("Government Spending", value);
              } else if (lineDesc.contains("Net exports")) {
                previousYearMetrics.put("Net Exports", value);
              }
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Could not load previous year data from {}: {}", previousYearPath, e.getMessage());
        }
      }

      // Add annual metrics with percent changes
      for (Map.Entry<String, Double> entry : gdpMetrics.entrySet()) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("year", year);
        record.put("quarter", null);
        record.put("metric", entry.getKey());
        record.put("value", entry.getValue());

        // Calculate percent change if previous year data exists
        Double percentChange = null;
        if (previousYearMetrics.containsKey(entry.getKey())) {
          double currentValue = entry.getValue();
          double previousValue = previousYearMetrics.get(entry.getKey());
          if (previousValue != 0) {
            percentChange = ((currentValue - previousValue) / previousValue) * 100.0;
          }
        }
        record.put("percent_change", percentChange);
        record.put("seasonally_adjusted", "Y");
        dataRecords.add(record);
      }

      // Calculate and add GDP growth rate if we have both nominal and real GDP
      if (gdpMetrics.containsKey("Real GDP")) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("year", year);
        record.put("quarter", null);
        record.put("metric", "GDP Growth Rate");

        // Calculate actual growth rate from Real GDP
        Double growthRate = null;
        if (previousYearMetrics.containsKey("Real GDP")) {
          double currentRealGdp = gdpMetrics.get("Real GDP");
          double previousRealGdp = previousYearMetrics.get("Real GDP");
          if (previousRealGdp != 0) {
            growthRate = ((currentRealGdp - previousRealGdp) / previousRealGdp) * 100.0;
          }
        }

        record.put("value", growthRate != null ? growthRate : null);
        record.put("percent_change", growthRate);
        record.put("seasonally_adjusted", "Y");
        dataRecords.add(record);
      }
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("gdp_statistics");
    storageProvider.writeAvroParquet(targetFilePath, columns, dataRecords, "GdpStatistics", "GdpStatistics");
    LOGGER.debug("Converted {} GDP statistics records to parquet: {}", dataRecords.size(), targetFilePath);
  }

}
