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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Downloads and converts U.S. Treasury data to Parquet format.
 * Supports daily treasury yields and federal debt statistics.
 *
 * <p>Uses the Treasury Fiscal Data API which requires no authentication.
 */
public class TreasuryDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TreasuryDataDownloader.class);
  private static final String TREASURY_API_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/";
  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

  public TreasuryDataDownloader(String cacheDir, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    this(cacheDir, cacheDir, cacheStorageProvider, storageProvider, null);
  }

  public TreasuryDataDownloader(String cacheDir, String operatingDirectory, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, cacheDir, cacheStorageProvider, storageProvider, sharedManifest);
  }

  @Override protected long getMinRequestIntervalMs() {
    return 0; // Treasury API has no rate limit
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
   * Downloads all Treasury data for the specified year range.
   */
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    // Download treasury yields data
    downloadTreasuryYields(startYear, endYear);

    // Download federal debt data
    downloadFederalDebt(startYear, endYear);
  }

  /**
   * Gets the default start year from environment variables.
   */
  public static int getDefaultStartYear() {
    // Check for ECON-specific override
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
   */
  public static int getDefaultEndYear() {
    // Check for ECON-specific override
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
   * Downloads treasury yields using default date range from environment.
   */
  public String downloadTreasuryYields() throws IOException, InterruptedException {
    return downloadTreasuryYields(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads daily treasury yield curve data.
   * @return The storage path (local or S3) where the data was saved
   */
  public String downloadTreasuryYields(int startYear, int endYear) throws IOException, InterruptedException {
    // Download for each year separately to match FileSchema partitioning expectations
    String lastPath = null;
    for (int year = startYear; year <= endYear; year++) {
      String relativePath = buildPartitionPath("timeseries", DataFrequency.DAILY, year) + "/treasury_yields.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("treasury_yields", year, cacheParams, relativePath)) {
        LOGGER.info("Found cached treasury yields for year {} - skipping download", year);
        lastPath = relativePath;
        continue;
      }

      // Fetch data from Treasury API for this year
      LOGGER.info("Downloading treasury yields for year {}", year);

      String startDate = year + "-01-01";
      String endDate = year + "-12-31";

      String url = TREASURY_API_BASE + "v2/accounting/od/avg_interest_rates"
          + "?filter=record_date:gte:" + startDate
          + ",record_date:lte:" + endDate
          + "&sort=-record_date&page[size]=10000";

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        LOGGER.warn("Treasury API request failed for year {} with status: {}", year, response.statusCode());
        continue;
      }

      // Save to cache using base class helper
      saveToCache("treasury_yields", year, cacheParams, relativePath, response.body());
      lastPath = relativePath;
    }

    return lastPath;
  }

  /**
   * Downloads federal debt using default date range from environment.
   */
  public String downloadFederalDebt() throws IOException, InterruptedException {
    return downloadFederalDebt(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads federal debt statistics.
   * @return The storage path (local or S3) where the data was saved
   */
  public String downloadFederalDebt(int startYear, int endYear) throws IOException, InterruptedException {
    // Download for each year separately to match FileSchema partitioning expectations
    String lastPath = null;
    for (int year = startYear; year <= endYear; year++) {
      String relativePath = buildPartitionPath("timeseries", DataFrequency.DAILY, year) + "/federal_debt.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("federal_debt", year, cacheParams, relativePath)) {
        LOGGER.info("Found cached federal debt for year {} - skipping download", year);
        lastPath = relativePath;
        continue;
      }

      // Fetch debt to the penny data for this year
      LOGGER.info("Downloading federal debt data for year {}", year);

      String startDate = year + "-01-01";
      String endDate = year + "-12-31";

      String url = TREASURY_API_BASE + "v2/accounting/od/debt_to_penny"
          + "?filter=record_date:gte:" + startDate
          + ",record_date:lte:" + endDate
          + "&page[size]=10000";

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        LOGGER.warn("Treasury API request failed for year {} with status: {}", year, response.statusCode());
        continue;
      }

      // Save to cache using base class helper
      saveToCache("federal_debt", year, cacheParams, relativePath, response.body());
      lastPath = relativePath;
    }

    return lastPath;
  }

  private int parseMaturityMonths(String description) {
    // Parse maturity from Treasury API security descriptions
    // Handle explicit maturity descriptions first
    if (description.contains("30-Year")) return 360;
    if (description.contains("20-Year")) return 240;
    if (description.contains("10-Year")) return 120;
    if (description.contains("7-Year")) return 84;
    if (description.contains("5-Year")) return 60;
    if (description.contains("3-Year")) return 36;
    if (description.contains("2-Year")) return 24;
    if (description.contains("1-Year")) return 12;
    if (description.contains("6-Month")) return 6;
    if (description.contains("3-Month")) return 3;
    if (description.contains("1-Month")) return 1;
    if (description.contains("4-Week")) return 1;

    // Handle Treasury API security types (typical maturity ranges)
    if (description.contains("Treasury Bills")) return 3; // Bills are typically 3-month average
    if (description.contains("Treasury Notes")) return 60; // Notes are typically 2-10 years, use 5-year average
    if (description.contains("Treasury Bonds")) return 360; // Bonds are typically 20-30 years, use 30-year average
    if (description.contains("Treasury Floating Rate Notes") || description.contains("FRN")) return 24; // FRNs are typically 2-year
    if (description.contains("Treasury Inflation-Protected Securities") || description.contains("TIPS")) return 120; // TIPS vary, use 10-year average
    if (description.contains("Federal Financing Bank")) return 120; // FFB varies, use 10-year average
    if (description.contains("Total Marketable")) return 60; // Average of all marketable, use 5-year
    if (description.contains("Total Non-marketable")) return 60; // Average of all non-marketable, use 5-year
    if (description.contains("Total Interest-bearing Debt")) return 60; // Average of all debt, use 5-year

    return 0; // Unknown
  }

  private String formatMaturityLabel(int months) {
    if (months >= 12) {
      int years = months / 12;
      return years + "Y";
    } else {
      return months + "M";
    }
  }

  private void writeTreasuryYieldsParquet(List<TreasuryYield> yields, String targetFilePath) throws IOException {
    // Build data records (keep all transformation logic)
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (TreasuryYield yield : yields) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("date", yield.date);
      record.put("maturity_months", yield.maturityMonths);
      record.put("maturity_label", yield.maturityLabel);
      record.put("yield_percent", yield.avgInterestRate);
      record.put("yield_type", yield.securityType);
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractEconDataDownloader.loadTableColumns("treasury_yields");
    storageProvider.writeAvroParquet(targetFilePath, columns, dataRecords, "TreasuryYield", "TreasuryYield");
  }

  private void writeFederalDebtParquet(List<FederalDebt> debtRecords, String targetFilePath) throws IOException {
    // Build data records (keep all transformation logic)
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (FederalDebt debt : debtRecords) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("date", debt.date);
      record.put("debt_type", debt.debtType);
      record.put("amount_billions", debt.totalDebt);
      record.put("holder_category", debt.holderCategory);
      record.put("debt_held_by_public", debt.debtHeldByPublic);
      record.put("intragovernmental_holdings", debt.intragovDebt);
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractEconDataDownloader.loadTableColumns("federal_debt");
    storageProvider.writeAvroParquet(targetFilePath, columns, dataRecords, "FederalDebt", "FederalDebt");
  }



  // Data classes
  private static class TreasuryYield {
    String date;
    String securityType;
    String securityDesc;
    int maturityMonths;
    String maturityLabel;
    double avgInterestRate;
  }

  private static class FederalDebt {
    String date;
    String debtType;
    double totalDebt;
    double debtHeldByPublic;
    double intragovDebt;
    String holderCategory;
  }

  /**
   * Converts cached Treasury yields data to Parquet format.
   * This method is called by EconSchemaFactory after downloading data.
   *
   * <p>This method trusts FileSchema's conversion registry to prevent redundant conversions.
   * No defensive file existence check is needed here.
   *
   * @param sourceDirPath Path to directory containing cached Treasury JSON data
   * @param targetFilePath Target parquet file to create
   */
  public void convertToParquet(String sourceDirPath, String targetFilePath) throws IOException {
    LOGGER.info("Converting Treasury data from {} to parquet: {}", sourceDirPath, targetFilePath);

    List<TreasuryYield> yields = new ArrayList<>();

    // Read treasury yields JSON file from cache using cacheStorageProvider
    // Treasury data is stored with frequency=daily partition: type=timeseries/frequency=daily/year=YYYY
    // sourceDirPath is like: <base>/type=timeseries/year=2020
    // We need to insert frequency=daily between type and year

    int yearIndex = sourceDirPath.lastIndexOf("/year=");
    if (yearIndex == -1) {
      throw new IOException("Cannot find year partition in source path: " + sourceDirPath);
    }

    String basePath = sourceDirPath.substring(0, yearIndex);  // Everything before /year=
    String yearDir = sourceDirPath.substring(yearIndex + 1);  // Skip the leading /: "year=2020"

    // Build correct path: base/frequency=daily/year=YYYY/treasury_yields.json
    String pathWithFrequency = cacheStorageProvider.resolvePath(basePath, "frequency=daily");
    String pathWithYear = cacheStorageProvider.resolvePath(pathWithFrequency, yearDir);
    String jsonFilePath = cacheStorageProvider.resolvePath(pathWithYear, "treasury_yields.json");

    if (!cacheStorageProvider.exists(jsonFilePath)) {
      throw new IOException("Source JSON file not found: " + jsonFilePath
          + ". Cannot convert to parquet without source data.");
    }

    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);
      JsonNode data = root.get("data");

      if (data == null || !data.isArray()) {
        throw new IOException("Invalid Treasury JSON format in " + jsonFilePath
            + ": missing or invalid 'data' array");
      }

      for (JsonNode record : data) {
        TreasuryYield yield = new TreasuryYield();
        yield.date = record.get("record_date").asText();
        yield.securityType = record.get("security_type_desc").asText("");
        yield.securityDesc = record.get("security_desc").asText("");
        yield.avgInterestRate = record.get("avg_interest_rate_amt").asDouble(0.0);

        // Parse maturity from description
        yield.maturityMonths = parseMaturityMonths(yield.securityDesc);
        yield.maturityLabel = formatMaturityLabel(yield.maturityMonths);

        yields.add(yield);
      }
    } catch (IOException e) {
      throw e; // Re-throw IOExceptions
    } catch (Exception e) {
      throw new IOException("Error reading Treasury JSON file " + jsonFilePath + ": " + e.getMessage(), e);
    }

    if (yields.isEmpty()) {
      throw new IOException("No treasury yield records found in " + jsonFilePath
          + ". Refusing to create empty parquet file.");
    }

    // Create parquet file
    LOGGER.info("=== TREASURY CONVERSION DEBUG ===");
    LOGGER.info("Raw JSON file: {}", jsonFilePath);
    LOGGER.info("Records parsed: {}", yields.size());
    LOGGER.info("Target parquet location: {}", targetFilePath);
    writeTreasuryYieldsParquet(yields, targetFilePath);
    LOGGER.info("Parquet file written successfully to: {}", targetFilePath);
    LOGGER.info("=== CONVERSION COMPLETE ===");

    // FileSchema's conversion registry automatically tracks this conversion
  }

  /**
   * Converts cached federal debt data to Parquet format.
   *
   * <p>This method trusts FileSchema's conversion registry to prevent redundant conversions.
   * No defensive file existence check is needed here.
   *
   * @param sourceDirPath Path to directory containing cached federal debt JSON data
   * @param targetFilePath Target parquet file path to create
   */
  public void convertFederalDebtToParquet(String sourceDirPath, String targetFilePath) throws IOException {
    LOGGER.info("Converting federal debt data from {} to parquet: {}", sourceDirPath, targetFilePath);

    List<FederalDebt> debtRecords = new ArrayList<>();

    // Read federal debt JSON file from cache using cacheStorageProvider
    // Federal debt data is stored with frequency=daily partition: type=timeseries/frequency=daily/year=YYYY
    // sourceDirPath is like: <base>/type=timeseries/year=2020
    // We need to insert frequency=daily between type and year

    int yearIndex = sourceDirPath.lastIndexOf("/year=");
    if (yearIndex == -1) {
      throw new IOException("Cannot find year partition in source path: " + sourceDirPath);
    }

    String basePath = sourceDirPath.substring(0, yearIndex);  // Everything before /year=
    String yearDir = sourceDirPath.substring(yearIndex + 1);  // Skip the leading /: "year=2020"

    // Build correct path: base/frequency=daily/year=YYYY/federal_debt.json
    String pathWithFrequency = cacheStorageProvider.resolvePath(basePath, "frequency=daily");
    String pathWithYear = cacheStorageProvider.resolvePath(pathWithFrequency, yearDir);
    String jsonFilePath = cacheStorageProvider.resolvePath(pathWithYear, "federal_debt.json");

    if (!cacheStorageProvider.exists(jsonFilePath)) {
      throw new IOException("Source JSON file not found: " + jsonFilePath
          + ". Cannot convert to parquet without source data.");
    }

    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);
      JsonNode data = root.get("data");

      if (data == null || !data.isArray()) {
        throw new IOException("Invalid federal debt JSON format in " + jsonFilePath
            + ": missing or invalid 'data' array");
      }

      for (JsonNode record : data) {
        FederalDebt debt = new FederalDebt();
        debt.date = record.get("record_date").asText();
        debt.debtType = "Total Public Debt Outstanding";

        // Parse debt amounts (convert from millions to billions)
        JsonNode totalDebtNode = record.get("tot_pub_debt_out_amt");
        if (totalDebtNode != null) {
          debt.totalDebt = totalDebtNode.asDouble(0.0) / 1000.0; // Convert millions to billions
        }

        JsonNode publicDebtNode = record.get("debt_held_public_amt");
        if (publicDebtNode != null) {
          debt.debtHeldByPublic = publicDebtNode.asDouble(0.0) / 1000.0;
        }

        JsonNode intragovDebtNode = record.get("intragov_hold_amt");
        if (intragovDebtNode != null) {
          debt.intragovDebt = intragovDebtNode.asDouble(0.0) / 1000.0;
        }

        debt.holderCategory = "All";

        debtRecords.add(debt);
      }
    } catch (IOException e) {
      throw e; // Re-throw IOExceptions
    } catch (Exception e) {
      throw new IOException("Error reading federal debt JSON file " + jsonFilePath + ": " + e.getMessage(), e);
    }

    if (debtRecords.isEmpty()) {
      throw new IOException("No federal debt records found in " + jsonFilePath
          + ". Refusing to create empty parquet file.");
    }

    // Create parquet file
    writeFederalDebtParquet(debtRecords, targetFilePath);
    LOGGER.info("Converted federal debt data to parquet: {} ({} records)", targetFilePath, debtRecords.size());

    // FileSchema's conversion registry automatically tracks this conversion
  }

}
