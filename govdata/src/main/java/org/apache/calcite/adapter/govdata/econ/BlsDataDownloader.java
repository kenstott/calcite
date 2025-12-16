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
import org.apache.calcite.adapter.govdata.BulkDownloadConfig;
import org.apache.calcite.adapter.govdata.CacheKey;
import org.apache.calcite.adapter.govdata.DuckDBCacheStore;
import org.apache.calcite.adapter.govdata.OperationType;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * Downloads and converts BLS economic data to Parquet format.
 * Supports employment statistics, inflation metrics, wage growth, and regional employment data.
 */
public class BlsDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlsDataDownloader.class);

  private final String apiKey;
  private final Set<String> enabledTables;

  // Helper methods for generating BLS series IDs
  public static class Series {
    /**
     * Generates BLS regional CPI series ID.
     * Format: CUUR{REGION}SA0
     * @param regionCode 4-digit region code (e.g., "0100" for Northeast)
     */
    public static String getRegionalCpiSeriesId(String regionCode) {
      return "CUUR" + regionCode + "SA0";
    }

    /**
     * Generates BLS state industry employment series ID.
     * Format: SMU{STATE_FIPS}{AREA}{SUPERSECTOR}{DATATYPE}
     * Example: SMU0600000000000001 = California Total Nonfarm Employment
     * @param stateFips 2-digit state FIPS code (e.g., "06" for CA)
     * @param supersector 8-digit NAICS supersector code
     */
    public static String getStateIndustryEmploymentSeriesId(String stateFips, String supersector) {
      return "SMU" + stateFips + "00000" + supersector + "01";
    }

    /**
     * Gets all state industry employment series IDs.
     * Generates series for all 51 jurisdictions × 22 supersectors = 1,122 series.
     */
    public static List<String> getAllStateIndustryEmploymentSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      for (String stateFips : BLS.stateFipsCodes.values()) {
        for (String supersector : BLS.naicsSupersectors.keySet()) {
          seriesIds.add(getStateIndustryEmploymentSeriesId(stateFips, supersector));
        }
      }
      return seriesIds;
    }

    /**
     * Gets state name from FIPS code.
     */
    public static String getStateName(String fipsCode) {
      for (Map.Entry<String, String> entry : BLS.stateFipsCodes.entrySet()) {
        if (entry.getValue().equals(fipsCode)) {
          return entry.getKey();
        }
      }
      return "Unknown State";
    }

    /**
     * Generates BLS metro area industry employment series ID.
     * Format: SMU{STATE}{AREA}{SUPERSECTOR}{DATATYPE}
     * Example: SMU3693561000000000001 for NYC Total Nonfarm
     *
     * @param metroCode Metro area code (e.g., "A100" for NYC)
     * @param supersector NAICS supersector code (e.g., "00000000" for total nonfarm)
     * @return BLS metro industry employment series ID
     */
    public static String getMetroIndustryEmploymentSeriesId(String metroCode, String supersector) {
      String blsAreaCode = BLS.metroBlsAreaCodes.get(metroCode);
      if (blsAreaCode == null) {
        throw new IllegalArgumentException("Unknown metro code: " + metroCode);
      }
      return "SMU" + blsAreaCode + supersector + "01";
    }

    /**
     * Gets all metro industry employment series IDs for the 27 major metro areas.
     * Generates 594 series (27 metros × 22 sectors).
     */
    public static List<String> getAllMetroIndustryEmploymentSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      for (String metroCode : BLS.metroCpiCodes.keySet()) {
        for (String supersector : BLS.naicsSupersectors.keySet()) {
          seriesIds.add(getMetroIndustryEmploymentSeriesId(metroCode, supersector));
        }
      }
      return seriesIds;
    }

  }

  // Lazily loaded from parquet catalogs - populated after downloadReferenceData() is called
  private volatile List<String> regionCodesList;
  private volatile Map<String, MetroGeography> metroGeographiesMap;
  private volatile boolean catalogsLoaded = false;

  /**
   * Metro geography data loaded from catalog.
   */
  private static class MetroGeography {
    final String metroCode;
    final String metroName;
    final String cpiAreaCode;  // null if no CPI data
    final String blsAreaCode;

    MetroGeography(String metroCode, String metroName, String cpiAreaCode, String blsAreaCode) {
      this.metroCode = metroCode;
      this.metroName = metroName;
      this.cpiAreaCode = cpiAreaCode;
      this.blsAreaCode = blsAreaCode;
    }
  }

  /**
   * BLS (Bureau of Labor Statistics) constants loaded from bls-constants.json resource file.
   * Provides strongly-typed access to all BLS configuration values including rate limits,
   * batching parameters, geographic codes, industry classifications, and series identifiers.
   *
   * <p>All values are loaded once on first access and cached for performance.
   * Uses Jackson POJO deserialization to eliminate imperative JSON parsing code.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class BlsConstants {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static volatile BlsConstants instance = null;

    // Rate limiting configuration
    public RateLimits rateLimits;

    // API batching limits
    public Batching batching;

    // Geographic codes (Jackson handles Map<String, String> automatically)
    public Map<String, String> stateFipsCodes;
    public Map<String, String> censusRegions;
    public Map<String, String> blsRegionCodes;  // 2-letter BLS region codes (NE, MW, SO, WE)
    public Map<String, String> metroBlsAreaCodes;

    // Metro CPI codes with metadata
    public Map<String, MetroCpiCode> metroCpiCodes;

    // Industry classifications
    public Map<String, String> naicsSupersectors;

    // Series IDs
    public SeriesIds seriesIds;

    // Table names
    public TableNames tableNames;

    /**
     * Rate limiting configuration POJO.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RateLimits {
      public long minRequestIntervalMs;
      public int maxRetries;
      public long retryDelayMs;
    }

    /**
     * API batching limits POJO.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Batching {
      public int maxSeriesPerRequest;
      public int maxYearsPerRequest;
    }

    /**
     * Metro CPI code with metadata POJO.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MetroCpiCode {
      public String cpiCode;  // null if no CPI data available
      public String name;
      public String comment;
    }

    /**
     * BLS series identifiers organized by category POJO.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SeriesIds {
      public Employment employment;
      public Inflation inflation;
      public Wages wages;

      @JsonIgnoreProperties(ignoreUnknown = true)
      public static class Employment {
        public String unemploymentRate;
        public String employmentLevel;
        public String laborForceParticipation;
      }

      @JsonIgnoreProperties(ignoreUnknown = true)
      public static class Inflation {
        public String cpiAllUrban;
        public String cpiCore;
        public String ppiFinalDemand;
      }

      @JsonIgnoreProperties(ignoreUnknown = true)
      public static class Wages {
        public String avgHourlyEarnings;
        public String employmentCostIndex;
      }
    }

    /**
     * BLS table name constants POJO.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TableNames {
      public String employmentStatistics;
      public String inflationMetrics;
      public String regionalCpi;
      public String metroCpi;
      public String stateIndustry;
      public String stateWages;
      public String countyWages;
      public String countyQcew;
      public String metroIndustry;
      public String metroWages;
      public String joltsRegional;
      public String joltsState;
      public String wageGrowth;
      public String regionalEmployment;
      public String referenceJoltsIndustries;
      public String referenceJoltsDataelements;
    }

    /**
     * Gets the singleton instance of BLS constants.
     * Loads from bls-constants.json resource on first access.
     */
    static BlsConstants getInstance() {
      if (instance == null) {
        synchronized (BlsConstants.class) {
          if (instance == null) {
            instance = loadFromResource();
          }
        }
      }
      return instance;
    }

    private static BlsConstants loadFromResource() {
      try (InputStream is = BlsConstants.class.getResourceAsStream("/bls/bls-constants.yaml")) {
        if (is == null) {
          throw new IOException("BLS constants resource not found: /bls/bls-constants.yaml");
        }
        // Parse YAML with anchor resolution using SnakeYAML, then convert to POJO
        com.fasterxml.jackson.databind.JsonNode parsedYaml =
            org.apache.calcite.adapter.govdata.YamlUtils.parseYamlOrJson(is, "/bls/bls-constants.yaml");
        return MAPPER.treeToValue(parsedYaml, BlsConstants.class);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load BLS constants from /bls/bls-constants.yaml", e);
      }
    }
  }

  public BlsDataDownloader(String apiKey, String cacheDir, String operatingDirectory, String parquetDirectory, StorageProvider cacheStorageProvider, StorageProvider storageProvider, CacheManifest sharedManifest, Set<String> enabledTables, int startYear, int endYear) {
    super(cacheDir, operatingDirectory, parquetDirectory, cacheStorageProvider, storageProvider, sharedManifest, startYear, endYear, null);
    this.apiKey = apiKey;
    this.enabledTables = enabledTables;
    // Catalogs are loaded lazily from parquet files after downloadReferenceData() generates them
  }

  /**
   * Ensures catalogs are loaded from parquet files.
   * Called lazily when catalog data is first needed.
   * By this point, downloadReferenceData() should have already generated the parquet files.
   */
  private synchronized void ensureCatalogsLoaded() {
    if (catalogsLoaded) {
      return;
    }
    try {
      this.regionCodesList = loadRegionCodesFromCatalog();
      this.metroGeographiesMap = loadMetroGeographiesFromCatalog();
      this.catalogsLoaded = true;
      LOGGER.info("Loaded BLS catalogs: {} regions, {} metros",
          regionCodesList.size(), metroGeographiesMap.size());
    } catch (Exception e) {
      throw new RuntimeException("Failed to load BLS reference catalogs from parquet files. "
          + "This typically indicates the reference data hasn't been generated yet. "
          + "Ensure downloadReferenceData() is called before accessing catalog data. "
          + "Original error: " + e.getMessage(), e);
    }
  }

  /**
   * Gets region codes list, loading from parquet if needed.
   */
  private List<String> getRegionCodesList() {
    ensureCatalogsLoaded();
    return regionCodesList;
  }

  /**
   * Gets metro geographies map, loading from parquet if needed.
   */
  private Map<String, MetroGeography> getMetroGeographiesMap() {
    ensureCatalogsLoaded();
    return metroGeographiesMap;
  }

  // BLS constants loaded from JSON resource
  public static final BlsConstants BLS = BlsConstants.getInstance();

  @Override protected String getTableName() {
    return BLS.tableNames.employmentStatistics;
  }

  @Override protected long getMinRequestIntervalMs() {
    return BLS.rateLimits.minRequestIntervalMs;
  }

  @Override protected int getMaxRetries() {
    return BLS.rateLimits.maxRetries;
  }

  @Override protected long getRetryDelayMs() {
    return BLS.rateLimits.retryDelayMs;
  }

  /**
   * Validates BLS API response and saves to cache appropriately.
   *
   * <p>Handling:
   * <ul>
   *   <li>REQUEST_SUCCEEDED: Save data to cache, return true
   *   <li>404/No data: Save response to cache (creates empty parquet), return true
   *   <li>Rate limit/errors: Don't save, return false (will retry later)
   * </ul>
   *
   * @param dataType     Type of data being cached
   * @param year         Year of data
   * @param cacheParams  Additional cache parameters
   * @param relativePath Relative path for a cache file
   * @param rawJson      Raw JSON response from BLS API
   * @throws IOException If JSON parsing fails
   */
  private void validateAndSaveBlsResponse(String dataType, int year,
      Map<String, String> cacheParams, String relativePath, String rawJson) throws IOException {

    JsonNode response = MAPPER.readTree(rawJson);
    String status = response.path("status").asText("UNKNOWN");

    if ("REQUEST_SUCCEEDED".equals(status)) {
      // Check if data is actually present
      JsonNode results = response.path("Results");
      JsonNode series = results.path("series");

      if (series.isMissingNode() || !series.isArray() || series.isEmpty()) {
        // 404/No data - save anyway to create an empty parquet file
        LOGGER.info("No data available for {} year {} - saving empty response", dataType, year);
      }
      // Has data - save normally
      saveToCache(dataType, year, cacheParams, relativePath, rawJson);
      return;
    }

    // Error response (rate limit, server error, etc) - don't save
    JsonNode messageNode = response.path("message");
    String message = messageNode.isArray() && !messageNode.isEmpty()
        ? messageNode.get(0).asText()
        : messageNode.asText("No error message");
    LOGGER.warn("BLS API error for {} year {}: {} - {} (not cached, will retry)",
                dataType, year, status, message);
  }

  /**
   * Gets BLS API batching limit for series per request.
   */
  private static int getMaxSeriesPerRequest() {
    return BLS.batching.maxSeriesPerRequest;
  }

  /**
   * Gets BLS API batching limit for years per request.
   */
  private static int getMaxYearsPerRequest() {
    return BLS.batching.maxYearsPerRequest;
  }

  /**
   * Resolves a bulk download configuration from a table's download node.
   * Handles both direct download configurations and bulkDownload references.
   *
   * @param downloadNode The download JsonNode from table metadata
   * @return The resolved BulkDownloadConfig
   * @throws IllegalArgumentException if bulkDownload reference cannot be resolved
   */
  private BulkDownloadConfig resolveBulkDownload(JsonNode downloadNode) {
    if (downloadNode == null) {
      throw new IllegalArgumentException("downloadNode cannot be null");
    }

    LOGGER.debug("resolveBulkDownload called with downloadNode: {}", downloadNode);

    // Check if this uses a bulkDownload reference
    if (downloadNode.has("bulkDownload")) {
      String bulkDownloadName = downloadNode.get("bulkDownload").asText();
      LOGGER.debug("Resolving bulkDownload reference: {}", bulkDownloadName);

      // Load bulk downloads from schema
      Map<String, BulkDownloadConfig> bulkDownloads = loadBulkDownloads();
      BulkDownloadConfig bulkConfig = bulkDownloads.get(bulkDownloadName);

      if (bulkConfig == null) {
        throw new IllegalArgumentException(
            "Unknown bulkDownload reference: '" + bulkDownloadName + "'");
      }

      LOGGER.debug("Resolved bulkDownload '{}': {}", bulkDownloadName, bulkConfig);
      return bulkConfig;
    }

    throw new IllegalArgumentException(
        "downloadNode must have a 'bulkDownload' reference field");
  }

  /**
   * Batches a list of years into contiguous ranges of up to maxYears each.
   * Example: [2000, 2001, 2005, 2024, 2025] with max=20 → [[2000-2001], [2005], [2024-2025]]
   */
  private List<int[]> batchYearsIntoRanges(List<Integer> years, int maxYears) {
    if (years.isEmpty()) {
      return new ArrayList<>();
    }

    List<int[]> ranges = new ArrayList<>();
    int rangeStart = years.get(0);
    int rangeEnd = years.get(0);

    for (int i = 1; i < years.size(); i++) {
      int year = years.get(i);

      // Check if we can extend the current range
      if (year != rangeEnd + 1 || (year - rangeStart) >= maxYears) {
        // Start a new range
        ranges.add(new int[]{rangeStart, rangeEnd});
        rangeStart = year;
      }
      rangeEnd = year;  // Extend range
    }

    // Add final range
    ranges.add(new int[]{rangeStart, rangeEnd});
    return ranges;
  }

  /**
   * Creates dimension provider for JOLTS state table.
   * Provides dimensions: type, year, frequency.
   */
  private DimensionProvider createJoltsStateDimensions() {
    String tableName = BLS.tableNames.joltsState;
    return (dimensionName) -> {
      switch (dimensionName) {
        case "type": return List.of(tableName);
        case "year": return yearRange(this.startYear, this.endYear);
        case "frequency": return List.of("monthly");
        default: return null;
      }
    };
  }

  /**
   * Fetches BLS data optimally batched by series (50 max) and years (20 max),
   * then splits results by individual year for caching.
   *
   * @param tableName Table name to load API URL from schema metadata
   * @param seriesIds List of BLS series IDs to fetch
   * @param uncachedYears Specific years that need fetching (maybe non-contiguous)
   * @return Map of year → JSON response for that year
   * @throws IOException If API request fails
   * @throws InterruptedException If interrupted while waiting
   */
  private Map<Integer, String> fetchAndSplitByYear(String tableName, List<String> seriesIds, List<Integer> uncachedYears)
      throws IOException, InterruptedException {

    Map<Integer, String> resultsByYear = new HashMap<>();

    // Batch uncached years into contiguous ranges
    List<int[]> yearRanges = batchYearsIntoRanges(uncachedYears, getMaxYearsPerRequest());

    LOGGER.info("Optimized fetch: {} series across {} years in {} batches",
                seriesIds.size(), uncachedYears.size(), yearRanges.size());

    // Batch by series (50 at a time)
    int maxSeriesPerReq = getMaxSeriesPerRequest();
    for (int seriesOffset = 0; seriesOffset < seriesIds.size(); seriesOffset += maxSeriesPerReq) {
      int seriesEnd = Math.min(seriesOffset + maxSeriesPerReq, seriesIds.size());
      List<String> seriesBatch = seriesIds.subList(seriesOffset, seriesEnd);

      // Fetch each year range
      for (int[] range : yearRanges) {
        int yearStart = range[0];
        int yearEnd = range[1];

        LOGGER.debug("Fetching series {}-{} for years {}-{}",
                    seriesOffset + 1, seriesEnd, yearStart, yearEnd);

        // Single API call for up to 50 series × up to 20 contiguous years
        String batchJson = fetchMultipleSeriesRaw(tableName, seriesBatch);
        JsonNode batchResponse = MAPPER.readTree(batchJson);

        // Split response by year for individual caching
        splitResponseByYear(batchResponse, resultsByYear, yearStart, yearEnd);
      }
    }

    return resultsByYear;
  }

  /**
   * Splits a multi-year BLS API response into per-year JSON responses.
   * Merges data if multiple batches cover the same year.
   */
  private void splitResponseByYear(JsonNode batchResponse, Map<Integer, String> resultsByYear,
      int startYear, int endYear) throws IOException {

    String status = batchResponse.path("status").asText("UNKNOWN");
    JsonNode seriesArray = batchResponse.path("Results").path("series");

    if (!seriesArray.isArray()) {
      LOGGER.warn("BLS API response missing 'Results.series' array for years {}-{}. Full response: {}",
          startYear, endYear, batchResponse.toPrettyString());
      return;
    }

    // Group series data by year
    Map<Integer, Map<String, ArrayNode>> dataByYear = new HashMap<>();

    LOGGER.debug("BLS API returned {} series for years {}-{}",
        seriesArray.size(), startYear, endYear);

    for (JsonNode series : seriesArray) {
      String seriesId = series.path("seriesID").asText();
      JsonNode dataArray = series.path("data");

      if (!dataArray.isArray()) {
        LOGGER.debug("Series {} has no data array", seriesId);
        continue;
      }

      LOGGER.debug("Series {} has {} data points", seriesId, dataArray.size());

      // Split series data points by year
      Set<Integer> yearsInSeries = new HashSet<>();
      int filteredCount = 0;
      for (JsonNode dataPoint : dataArray) {
        int year = dataPoint.path("year").asInt();

        yearsInSeries.add(year);

        if (year >= startYear && year <= endYear) {
          dataByYear.computeIfAbsent(year, k -> new HashMap<>())
                    .computeIfAbsent(seriesId, k -> MAPPER.createArrayNode())
                    .add(dataPoint);
        } else {
          filteredCount++;
        }
      }

      if (filteredCount > 0) {
        LOGGER.debug("Series {}: filtered {} data points outside range {}-{}",
            seriesId, filteredCount, startYear, endYear);
      }
      LOGGER.debug("Series {} contains data for years: {}", seriesId, yearsInSeries);
    }

    LOGGER.debug("Data aggregated for {} years out of requested range {}-{}. Years with data: {}",
        dataByYear.size(), startYear, endYear, dataByYear.keySet());

    // Create per-year JSON responses
    for (int year = startYear; year <= endYear; year++) {
      Map<String, ArrayNode> yearSeriesData = dataByYear.get(year);
      if (yearSeriesData == null || yearSeriesData.isEmpty()) {
        LOGGER.warn("No series data returned for year {} (requested {}-{}). " +
            "Years that DID have data: {}. Check if this year's data exists in BLS database.",
            year, startYear, endYear, dataByYear.keySet());
        continue;
      }

      // Build JSON response matching BLS structure
      ObjectNode yearResponse = MAPPER.createObjectNode();
      yearResponse.put("status", status);
      ObjectNode results = yearResponse.putObject("Results");
      ArrayNode seriesOutput = results.putArray("series");

      // Add each series with its year-filtered data
      for (Map.Entry<String, ArrayNode> entry : yearSeriesData.entrySet()) {
        ObjectNode seriesNode = MAPPER.createObjectNode();
        seriesNode.put("seriesID", entry.getKey());
        seriesNode.set("data", entry.getValue());
        seriesOutput.add(seriesNode);
      }

      String yearJson = MAPPER.writeValueAsString(yearResponse);

      // Merge if year already has data from the previous batch
      final int currentYear = year; // For lambda
      resultsByYear.merge(year, yearJson, (existing, newData) -> {
        try {
          return mergeBatchResponses(existing, newData);
        } catch (IOException e) {
          LOGGER.warn("Failed to merge batch responses for year {}: {}", currentYear, e.getMessage());
          return existing;
        }
      });
    }
  }

  /**
   * Merges two BLS API JSON responses (for the same year, different series batches).
   */
  private String mergeBatchResponses(String json1, String json2) throws IOException {
    JsonNode response1 = MAPPER.readTree(json1);
    JsonNode response2 = MAPPER.readTree(json2);

    ObjectNode merged = MAPPER.createObjectNode();
    merged.put("status", response1.path("status").asText());
    ObjectNode results = merged.putObject("Results");
    ArrayNode mergedSeries = results.putArray("series");

    // Add all series from both responses
    ArrayNode series1 = (ArrayNode) response1.path("Results").path("series");
    ArrayNode series2 = (ArrayNode) response2.path("Results").path("series");

    for (JsonNode series : series1) {
      mergedSeries.add(series);
    }
    for (JsonNode series : series2) {
      mergedSeries.add(series);
    }

    return MAPPER.writeValueAsString(merged);
  }

  /**
   * Fetches and splits data for large series lists (>50 series) with year batching.
   * Handles both series batching (50 at a time) and year batching (20 at a time).
   *
   * @param tableName Table name to load API URL from schema metadata
   * @param seriesIds Full list of series IDs (can be >50)
   * @param uncachedYears List of years that need downloading
   * @return Map of year → combined JSON response
   */
  private Map<Integer, String> fetchAndSplitByYearLargeSeries(String tableName, List<String> seriesIds, List<Integer> uncachedYears)
      throws IOException, InterruptedException {

    Map<Integer, String> resultsByYear = new HashMap<>();
    List<int[]> yearRanges = batchYearsIntoRanges(uncachedYears, getMaxYearsPerRequest());

    int maxSeriesPerReq = getMaxSeriesPerRequest();
    LOGGER.info("Optimized fetch: {} series across {} years in {} year-batches, {} series-batches",
                seriesIds.size(), uncachedYears.size(), yearRanges.size(),
                (seriesIds.size() + maxSeriesPerReq - 1) / maxSeriesPerReq);

    // Batch series into groups of 50
    for (int seriesOffset = 0; seriesOffset < seriesIds.size(); seriesOffset += maxSeriesPerReq) {
      int seriesEnd = Math.min(seriesOffset + maxSeriesPerReq, seriesIds.size());
      List<String> seriesBatch = seriesIds.subList(seriesOffset, seriesEnd);

      LOGGER.info("Processing series batch {}/{} ({} series)",
                  (seriesOffset / maxSeriesPerReq) + 1,
                  (seriesIds.size() + maxSeriesPerReq - 1) / maxSeriesPerReq,
                  seriesBatch.size());

      // Fetch each year range for this series batch
      for (int[] range : yearRanges) {
        int yearStart = range[0];
        int yearEnd = range[1];

        String batchJson = fetchMultipleSeriesRaw(tableName, seriesBatch);
        JsonNode batchResponse = MAPPER.readTree(batchJson);

        // Check for errors
        String status = batchResponse.path("status").asText("UNKNOWN");
        if (!"REQUEST_SUCCEEDED".equals(status)) {
          JsonNode messageNode = batchResponse.path("message");
          String message = messageNode.isArray() && !messageNode.isEmpty()
              ? messageNode.get(0).asText()
              : messageNode.asText("No error message");

          LOGGER.warn("BLS API error for year range {}-{}: {} - {}", yearStart, yearEnd, status, message);

          // Check for rate limit
          if ("REQUEST_NOT_PROCESSED".equals(status)
              && (message.contains("daily threshold") || message.contains("rate limit"))) {
            LOGGER.warn("BLS API rate limit reached. Returning partial results.");
            return resultsByYear; // Return what we have so far
          }
          continue; // Skip this batch
        }

        // Split response by year and merge with existing data
        splitResponseByYear(batchResponse, resultsByYear, yearStart, yearEnd);
      }
    }

    return resultsByYear;
  }

  /**
   * Downloads all BLS data for the specified year range.
   * Uses the enabledTables set passed to the constructor to filter which tables to download.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @throws IOException If download or file I/O fails
   * @throws InterruptedException If download is interrupted
   */
  @Override public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    // Use instance variables instead of parameters for consistent year range across all tables
    downloadAllTables(this.startYear, this.endYear, enabledTables);
  }

  /**
   * Downloads BLS data for the specified year range, filtered by table names.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @param enabledTables Set of table names to download, or null to download all tables.
   *                      If provided, only tables in this set will be downloaded.
   */
  private void downloadAllTables(int startYear, int endYear, Set<String> enabledTables) throws IOException {
    // Download employment statistics
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.employmentStatistics)) {
      downloadEmploymentStatistics();
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.employmentStatistics);
    }

    // Download inflation metrics
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.inflationMetrics)) {
      downloadInflationMetrics();
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.inflationMetrics);
    }

    // Download regional CPI
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.regionalCpi)) {
      downloadRegionalCpi(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.regionalCpi);
    }

    // Download metro CPI
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.metroCpi)) {
      downloadMetroCpi(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.metroCpi);
    }

    // Download state industry employment
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.stateIndustry)) {
      downloadStateIndustryEmployment(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out - saves ~1,122 series!)", BLS.tableNames.stateIndustry);
    }

    // Download state wages
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.stateWages)) {
      downloadStateWages(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.stateWages);
    }

    // Download county wages
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.countyWages)) {
      downloadCountyWages(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out - saves ~6,000 counties!)", BLS.tableNames.countyWages);
    }

    // Download county QCEW (comprehensive county-level employment and wage data)
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.countyQcew)) {
      downloadCountyQcew(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.countyQcew);
    }

    // Download metro industry employment
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.metroIndustry)) {
      downloadMetroIndustryEmployment(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out - saves ~594 series!)", BLS.tableNames.metroIndustry);
    }

    // Download metro wages
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.metroWages)) {
      downloadMetroWages();
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.metroWages);
    }

    // Download JOLTS regional data
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.joltsRegional)) {
      downloadJoltsRegional();
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.joltsRegional);
    }

    // Download JOLTS state data
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.joltsState)) {
      downloadJoltsState();
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.joltsState);
    }

    // Download wage growth data
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.wageGrowth)) {
      downloadWageGrowth();
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.wageGrowth);
    }

    // Download regional employment data
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.regionalEmployment)) {
      downloadRegionalEmployment();
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.regionalEmployment);
    }

    // Download reference tables (always downloaded, not subject to filtering)
    LOGGER.debug("Processing JOLTS reference tables (industries, data elements)");
    downloadJoltsIndustries();
    downloadJoltsDataelements();
    LOGGER.info("BLS data download completed");
  }

  /**
   * Converts all downloaded BLS data to Parquet format for the specified year range.
   * Uses the enabledTables set passed to the constructor to filter which tables to convert.
   * Uses IterationDimension pattern for declarative multidimensional iteration.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   */
  @Override public void convertAll(int startYear, int endYear) {
    // Use instance variables instead of parameters for consistent year range
    LOGGER.info("Converting BLS data for years {}-{}", this.startYear, this.endYear);

    // Define all BLS tables (12 tables)
    List<String> tablesToConvert =
        Arrays.asList(BLS.tableNames.employmentStatistics,
        BLS.tableNames.inflationMetrics,
        BLS.tableNames.regionalCpi,
        BLS.tableNames.metroCpi,
        BLS.tableNames.stateIndustry,
        BLS.tableNames.stateWages,
        BLS.tableNames.metroIndustry,
        BLS.tableNames.metroWages,
        BLS.tableNames.joltsRegional,
        BLS.tableNames.countyWages,
        BLS.tableNames.joltsState,
        BLS.tableNames.wageGrowth);

    // Convert each enabled table using IterationDimension pattern
    for (String tableName : tablesToConvert) {
      if (enabledTables == null || enabledTables.contains(tableName)) {
        // metro_wages uses direct CSV→Parquet conversion (no intermediate JSON)
        if (BLS.tableNames.metroWages.equals(tableName)) {
          convertMetroWagesAll();
          continue;
        }

        // Dimension provider - metadata-driven tables (employment_statistics, state_wages, jolts_regional)
        // only need fallback for dimensions not in metadata. Other tables use legacy provider.
        DimensionProvider dimensionProvider = getDimensionProvider(tableName);

        // Other tables use JSON→Parquet conversion with DuckDB bulk cache filtering (10-20x faster)
        iterateTableOperationsOptimized(
            tableName,
            dimensionProvider,
            null,  // No prefetch for conversion
            (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
              // Execute conversion - only mark as converted if successful
              boolean converted = convertCachedJsonToParquet(tableName, vars);

              if (converted) {
                // Mark as converted in manifest
                cacheManifest.markParquetConverted(cacheKey, parquetPath);
              } else {
                LOGGER.warn("Conversion failed for {} ({}), not marking as converted",
                    tableName, cacheKey);
              }
            },
            OperationType.CONVERSION);
      }
    }

    LOGGER.info("BLS conversion complete for all enabled tables");
  }

  private DimensionProvider getDimensionProvider(String tableName) {
    DimensionProvider dimensionProvider;
    if (BLS.tableNames.employmentStatistics.equals(tableName)
        || BLS.tableNames.stateWages.equals(tableName)
        || BLS.tableNames.joltsRegional.equals(tableName)) {
      // Pilot tables: dimensions defined in YAML metadata, no fallback needed
      dimensionProvider = createMetadataDimensionProvider(tableName, (dim) -> null);
    } else if (BLS.tableNames.joltsState.equals(tableName)) {
      dimensionProvider = createJoltsStateDimensions();
    } else {
      // Default dimension provider for tables without metadata
      dimensionProvider = (dimensionName) -> {
        if ("year".equals(dimensionName)) {
          return yearRange(this.startYear, this.endYear);
        }
        if ("type".equals(dimensionName)) return List.of(tableName);
        if ("frequency".equals(dimensionName)) return List.of("monthly");
        return null;
      };
    }
    return dimensionProvider;
  }

  /**
   * Downloads employment statistics data and converts to Parquet.
   */
  public void downloadEmploymentStatistics() {

    // Series IDs to fetch (constant across all years)
    final List<String> seriesIds =
        List.of(BLS.seriesIds.employment.unemploymentRate,
        BLS.seriesIds.employment.employmentLevel,
        BLS.seriesIds.employment.laborForceParticipation);

    String tableName = BLS.tableNames.employmentStatistics;

    // Create persistent prefetch connection that survives both download and conversion
    Connection prefetchDb = null;
    PrefetchHelper prefetchHelper;
    try {
      prefetchDb = getDuckDBConnection(storageProvider);

      // Load table metadata and extract partition keys
      Map<String, Object> metadata = loadTableMetadata(tableName);
      String pattern = (String) metadata.get("pattern");
      List<String> partitionKeys = extractPartitionKeysFromPattern(pattern);

      // Create prefetch cache table
      createPrefetchCacheTable(prefetchDb, tableName, partitionKeys, metadata);
      prefetchHelper = new PrefetchHelper(prefetchDb, tableName + "_prefetch", partitionKeys);

    } catch (Exception e) {
      LOGGER.error("Failed to initialize prefetch infrastructure for {}: {}", tableName, e.getMessage());
      if (prefetchDb != null) {
        try {
          prefetchDb.close();
        } catch (Exception closeEx) {
          // Ignore
        }
      }
      throw new RuntimeException("Failed to initialize prefetch for " + tableName, e);
    }

    final Connection finalPrefetchDb = prefetchDb;
    final PrefetchHelper finalPrefetchHelper = prefetchHelper;

    try {
      // DOWNLOAD: Fetch data and populate prefetch cache
      // Note: employment_statistics uses metadata-driven dimensions (YAML)
      iterateTableOperationsOptimized(
          tableName,
          (dim) -> null,  // Let the method create metadata-aware provider with correct years
          // PREFETCH CALLBACK - Load cached data OR fetch from API
          (context, helper) -> {
            if ("year".equals(context.segmentDimensionName)) {
              List<String> years = context.allDimensionValues.get("year");
              LOGGER.info("Prefetching employment data for {} years (cache + API)", years.size());

              // Separate years into cached vs uncached
              List<Integer> uncachedYears = new ArrayList<>();
              List<Map<String, String>> cachedPartitions = new ArrayList<>();
              List<String> cachedJsons = new ArrayList<>();

              for (String yearStr : years) {
                int year = Integer.parseInt(yearStr);
                Map<String, String> params = Map.of("year", yearStr, "frequency", "M");
                CacheKey cacheKey = new CacheKey(tableName, params);

                if (cacheManifest.isCached(cacheKey)) {
                  // Load from cache
                  String jsonPath = resolveJsonPath(tableName, params);
                  String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
                  try (InputStream is = cacheStorageProvider.openInputStream(fullPath)) {
                    byte[] bytes = new byte[is.available()];
                    int bytesRead = is.read(bytes);
                    LOGGER.debug("Read {} bytes from {}", bytesRead, fullPath);
                    String jsonContent = new String(bytes, StandardCharsets.UTF_8);
                    cachedPartitions.add(params);
                    cachedJsons.add(jsonContent);
                  } catch (Exception e) {
                    LOGGER.warn("Failed to load cached JSON for year {}, will re-fetch: {}", year, e.getMessage());
                    uncachedYears.add(year);
                  }
                } else {
                  uncachedYears.add(year);
                }
              }

              // Add cached data to prefetch table
              if (!cachedPartitions.isEmpty()) {
                helper.insertJsonBatch(cachedPartitions, cachedJsons);
                LOGGER.info("Loaded {} cached years into prefetch table", cachedPartitions.size());
              }

              // Fetch uncached years from API in ONE batch call
              if (!uncachedYears.isEmpty()) {
                LOGGER.info("Fetching {} uncached years from BLS API", uncachedYears.size());
                Map<Integer, String> apiData = fetchAndSplitByYear(tableName, seriesIds, uncachedYears);

                List<Map<String, String>> apiPartitions = new ArrayList<>();
                List<String> apiJsons = new ArrayList<>();
                for (Map.Entry<Integer, String> entry : apiData.entrySet()) {
                  apiPartitions.add(Map.of("type", tableName, "year", String.valueOf(entry.getKey()), "frequency", "M"));
                  apiJsons.add(entry.getValue());
                }
                helper.insertJsonBatch(apiPartitions, apiJsons);
                LOGGER.info("Fetched {} years from API into prefetch table", apiData.size());
              }
            }
          },
          // TABLE OPERATION - Convert cached JSON to parquet
          (cacheKey, vars, jsonPath, parquetPath, helper) -> {
            String rawJson = helper.getJson(vars);

            if (rawJson != null) {
              int year = Integer.parseInt(vars.get("year"));
              String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
              validateAndSaveBlsResponse(tableName, year, vars, fullJsonPath, rawJson);
            }
          },
          OperationType.DOWNLOAD,
          finalPrefetchDb,
          finalPrefetchHelper);

    } finally {
      // Close prefetch DB after both download and conversion complete
      if (prefetchDb != null) {
        try {
          prefetchDb.close();
        } catch (Exception e) {
          LOGGER.warn("Failed to close prefetch DB: {}", e.getMessage());
        }
      }
    }
  }

  /**
   * Downloads CPI data for 4 Census regions (Northeast, Midwest, South, West).
   * Uses catalog-driven pattern with iterateTableOperationsOptimized() for 10-20x performance.
   */
  public void downloadRegionalCpi(int startYear, int endYear) {
    String tableName = BLS.tableNames.regionalCpi;

    LOGGER.debug("Processing regional CPI for {} Census regions for years {}-{}",
        getRegionCodesList().size(), startYear, endYear);

    // Build list of series IDs from catalog
    List<String> seriesIds = new ArrayList<>();
    for (String regionCode : getRegionCodesList()) {
      seriesIds.add(Series.getRegionalCpiSeriesId(regionCode));
    }

    // Use optimized iteration with DuckDB bulk cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "type": return List.of(tableName);
            case "year": return yearRange(this.startYear, this.endYear);
            case "frequency": return List.of("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch all regions for this year
          List<Integer> singleYearList = Collections.singletonList(year);
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(tableName, seriesIds, singleYearList);

          String rawJson = resultsByYear.get(year);
          if (rawJson != null) {
            // Save to cache
            cacheStorageProvider.writeFile(jsonPath, rawJson.getBytes(StandardCharsets.UTF_8));
            long fileSize = cacheStorageProvider.getMetadata(jsonPath).getSize();

            cacheManifest.markCached(cacheKey, jsonPath, fileSize,
                getCacheExpiryForYear(year), getCachePolicyForYear(year));
          }
        },
        OperationType.DOWNLOAD);
  }

  /**
   * Downloads CPI data for major metro areas.
   * Uses metadata-driven pattern with series IDs from requestBody config.
   */
  @SuppressWarnings("unchecked")
  public void downloadMetroCpi(int startYear, int endYear) {
    String tableName = BLS.tableNames.metroCpi;

    // Get series IDs directly from requestBody config in schema metadata
    Map<String, Object> metadata = loadTableMetadata(tableName);
    JsonNode downloadConfig = (JsonNode) metadata.get("download");
    JsonNode requestBody = downloadConfig.get("requestBody");
    List<String> seriesIds = new ArrayList<>();
    for (JsonNode seriesId : requestBody.get("seriesid")) {
      seriesIds.add(seriesId.asText());
    }

    LOGGER.debug("Processing metro area CPI for {} series for years {}-{}",
        seriesIds.size(), startYear, endYear);
    LOGGER.info("Found {} metro CPI series from schema metadata", seriesIds.size());

    // Use optimized iteration with metadata-only dimensions
    iterateTableOperationsOptimized(
        tableName,
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch all metros for this year
          List<Integer> singleYearList = Collections.singletonList(year);
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(tableName, seriesIds, singleYearList);

          String rawJson = resultsByYear.get(year);
          if (rawJson != null) {
            // Save to cache
            cacheStorageProvider.writeFile(jsonPath, rawJson.getBytes(StandardCharsets.UTF_8));
            long fileSize = cacheStorageProvider.getMetadata(jsonPath).getSize();

            cacheManifest.markCached(cacheKey, jsonPath, fileSize,
                getCacheExpiryForYear(year), getCachePolicyForYear(year));
          }
        },
        OperationType.DOWNLOAD);
  }

  /**
   * Downloads employment-by-industry data for all 51 U.S. jurisdictions (50 states + DC)
   * across 22 NAICS supersector codes. Generates 1,122 series (51 × 22).
   *
   * <p>Optimized with year-batching to reduce API calls from ~345 (23 batches × 15 years)
   * to ~46 (23 batches × 2-year-batches).
   */
  public void downloadStateIndustryEmployment(int startYear, int endYear) {
    LOGGER.debug("Processing state industry employment for {} states × {} sectors ({} series) for {}-{}",
                BLS.stateFipsCodes.size(), BLS.naicsSupersectors.size(),
                BLS.stateFipsCodes.size() * BLS.naicsSupersectors.size(), startYear, endYear);

    final List<String> seriesIds = Series.getAllStateIndustryEmploymentSeriesIds();
    LOGGER.info("Generated {} state industry employment series IDs", seriesIds.size());
    LOGGER.info("State industry series examples: {}, {}, {}",
        !seriesIds.isEmpty() ? seriesIds.get(0) : "none",
                seriesIds.size() > 1 ? seriesIds.get(1) : "none",
                seriesIds.size() > 2 ? seriesIds.get(2) : "none");

    String tableName = BLS.tableNames.stateIndustry;

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "type": return List.of(tableName);
            case "year": return yearRange(this.startYear, this.endYear);
            case "frequency": return List.of("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch for this year (with large series batching)
          Map<Integer, String> resultsByYear = fetchAndSplitByYearLargeSeries(tableName, seriesIds, List.of(year));
          String rawJson = resultsByYear.get(year);

          if (rawJson == null) {
            LOGGER.warn("No data returned from API for {} year {} - skipping save", tableName, year);
          } else {
            validateAndSaveBlsResponse(tableName, year, vars, jsonPath, rawJson);
          }
        },
        OperationType.DOWNLOAD);

  }

  /**
   * Downloads average weekly wages for all 51 U.S. jurisdictions (50 states + DC)
   * from BLS QCEW (Quarterly Census of Employment and Wages) CSV files.
   *
   * <p>Note: QCEW (ENU series) data is not available through the BLS API v2.
   * This method downloads annual QCEW CSV files and extracts state-level wage data.
   * Uses agglvl_code 50 for state-level aggregation.
   */
  public void downloadStateWages(int startYear, int endYear) {
    LOGGER.debug("Processing state wages from QCEW CSV files for {}-{}", startYear, endYear);

    String tableName = BLS.tableNames.stateWages;

    // Load bulk download configuration once before iteration
    Map<String, Object> metadata = loadTableMetadata(tableName);
    JsonNode downloadNode = (JsonNode) metadata.get("download");
    if (downloadNode == null) {
      throw new IllegalStateException("No 'download' section found in metadata for table: " + tableName);
    }
    BulkDownloadConfig bulkConfig = resolveBulkDownload(downloadNode);

    // Note: state_wages uses metadata-driven dimensions (YAML)
    iterateTableOperationsOptimized(
        tableName,
        createMetadataDimensionProvider(tableName, (dim) -> null),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));
          String frequency = vars.get("frequency");

          // Resolve variables for this iteration
          Map<String, String> variableValues = new HashMap<>();
          variableValues.put("year", String.valueOf(year));
          variableValues.put("frequency", frequency);

          String qcewZipPath = bulkConfig.resolveCachePath(variableValues);
          String downloadUrl = bulkConfig.resolveUrl(variableValues);

          // Download QCEW CSV (reuses cache if available)
          downloadQcewCsvIfNeeded(year, qcewZipPath, downloadUrl);

          // Get full path to cached ZIP file
          String fullZipPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);

          // Parse CSV and convert to Parquet using DuckDB
          parseQcewForStateWages(fullZipPath, parquetPath, year, frequency);

          // Mark as converted in manifest
          cacheManifest.markParquetConverted(cacheKey, parquetPath);

          LOGGER.info("Completed state wages for year {}", year);
        },
        OperationType.CONVERSION);

  }

  /**
   * Downloads county-level wage data from QCEW annual CSV files and converts to Parquet.
   * Extracts data for ~6,038 counties (most granular wage data available).
   * Reuses the same QCEW CSV files already downloaded for state wages.
   */
  public void downloadCountyWages(int startYear, int endYear) {
    LOGGER.debug("Processing county wages from QCEW CSV files for {}-{}", startYear, endYear);

    String tableName = BLS.tableNames.countyWages;

    // Load bulk download configuration once before iteration
    Map<String, Object> metadata = loadTableMetadata(tableName);
    JsonNode downloadNode = (JsonNode) metadata.get("download");
    if (downloadNode == null) {
      throw new IllegalStateException("No 'download' section found in metadata for table: " + tableName);
    }
    BulkDownloadConfig bulkConfig = resolveBulkDownload(downloadNode);

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "type": return List.of(tableName);
            case "year": return yearRange(this.startYear, this.endYear);
            case "frequency": return List.of("qtrly", "annual");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));
          String frequency = vars.get("frequency");

          // Resolve variables for this iteration
          Map<String, String> variableValues = new HashMap<>();
          variableValues.put("year", String.valueOf(year));
          variableValues.put("frequency", frequency);

          String qcewZipPath = bulkConfig.resolveCachePath(variableValues);
          String downloadUrl = bulkConfig.resolveUrl(variableValues);

          // Download QCEW CSV (reuses cache from state_wages if available)
          downloadQcewCsvIfNeeded(year, qcewZipPath, downloadUrl);

          // Get full path to cached ZIP file
          String fullZipPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);

          // Parse CSV and convert to Parquet using DuckDB
          parseQcewForCountyWages(fullZipPath, parquetPath, year, frequency);

          // Mark as converted in manifest
          cacheManifest.markParquetConverted(cacheKey, parquetPath);

          LOGGER.info("Completed county wages for year {}", year);
        },
        OperationType.CONVERSION);

  }

  /**
   * Downloads county-level QCEW (Quarterly Census of Employment and Wages) data from BLS annual CSV files.
   * Extracts comprehensive county employment and wage data including establishment counts, employment levels,
   * total wages, and average weekly wages by industry (NAICS) and ownership type.
   *
   * <p>Reuses QCEW ZIP files already downloaded for state_wages and county_wages to avoid redundant downloads.
   * Produces detailed county-level labor market data for all ~3,142 U.S. counties with industry and ownership breakdowns.
   *
   * @param startYear Start year (inclusive)
   * @param endYear   End year (inclusive)
   */
  public void downloadCountyQcew(int startYear, int endYear) {
    LOGGER.debug("Processing county QCEW data from BLS CSV files for {}-{}", startYear, endYear);

    String tableName = BLS.tableNames.countyQcew;

    // Load bulk download configuration once before iteration
    Map<String, Object> metadata = loadTableMetadata(tableName);
    JsonNode downloadNode = (JsonNode) metadata.get("download");
    if (downloadNode == null) {
      throw new IllegalStateException("No 'download' section found in metadata for table: " + tableName);
    }
    BulkDownloadConfig bulkConfig = resolveBulkDownload(downloadNode);

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "type": return List.of(tableName);
            case "year": return yearRange(this.startYear, this.endYear);
            case "frequency": return List.of("qtrly", "annual");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));
          String frequency = vars.get("frequency");

          // Resolve variables for this iteration
          Map<String, String> variableValues = new HashMap<>();
          variableValues.put("year", String.valueOf(year));
          variableValues.put("frequency", frequency);

          String qcewZipPath = bulkConfig.resolveCachePath(variableValues);
          String downloadUrl = bulkConfig.resolveUrl(variableValues);

          // Download QCEW CSV (reuses cache from state_wages/county_wages if available)
          downloadQcewCsvIfNeeded(year, qcewZipPath, downloadUrl);

          // Get full path to cached ZIP file
          String fullZipPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);

          // Parse and convert to Parquet using DuckDB
          parseAndConvertQcewToParquet(fullZipPath, parquetPath, year, frequency);

          // Mark as converted in manifest
          cacheManifest.markParquetConverted(cacheKey, parquetPath);

          LOGGER.info("Completed county QCEW data for year {}", year);
        },
        OperationType.CONVERSION);

  }

  /**
   * Downloads employment-by-industry data for 27 major U.S. metropolitan areas
   * across 22 NAICS supersector codes. Generates 594 series (27 × 22).
   *
   * <p>Optimized with year-batching to reduce API calls from ~180 (12 batches × 15 years)
   * to ~24 (12 batches × 2-year-batches).
   */
  public void downloadMetroIndustryEmployment(int startYear, int endYear) {
    LOGGER.debug("Processing metro industry employment for {} metros × {} sectors ({} series) for {}-{}",
                BLS.metroCpiCodes.size(), BLS.naicsSupersectors.size(),
                BLS.metroCpiCodes.size() * BLS.naicsSupersectors.size(), startYear, endYear);

    final List<String> seriesIds = Series.getAllMetroIndustryEmploymentSeriesIds();
    LOGGER.info("Generated {} metro industry employment series IDs", seriesIds.size());
    LOGGER.info("Metro industry series examples: {}, {}, {}",
        !seriesIds.isEmpty() ? seriesIds.get(0) : "none",
                seriesIds.size() > 1 ? seriesIds.get(1) : "none",
                seriesIds.size() > 2 ? seriesIds.get(2) : "none");

    String tableName = BLS.tableNames.metroIndustry;

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "type": return List.of(tableName);
            case "year": return yearRange(this.startYear, this.endYear);
            case "frequency": return List.of("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch for this year (with large series batching)
          Map<Integer, String> resultsByYear = fetchAndSplitByYearLargeSeries(tableName, seriesIds, List.of(year));
          String rawJson = resultsByYear.get(year);

          if (rawJson == null) {
            LOGGER.warn("No data returned from API for {} year {} - skipping save", tableName, year);
          } else {
            validateAndSaveBlsResponse(tableName, year, vars, jsonPath, rawJson);
          }
        },
        OperationType.DOWNLOAD);

  }

  /**
   * Downloads QCEW bulk ZIP files containing metro wage data.
   *
   * <p>Downloads both annual and quarterly bulk files from BLS QCEW.
   * Conversion to Parquet is handled separately by {@link #convertMetroWagesAll()}
   * which uses DuckDB's zipfs extension to read CSV files directly from the ZIP archives.
   *
   * <p><b>Data Source:</b>
   * <pre>
   * Annual:    https://data.bls.gov/cew/data/files/{year}/csv/{year}_annual_singlefile.zip (~80MB)
   * Quarterly: https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip (~323MB)
   * </pre>
   *
   * <p><b>Coverage:</b>
   * 27 major metropolitan areas from 1990 to present.
   *
   * @see #downloadQcewBulkFile(int, String) For ZIP download implementation
   * @see #convertMetroWagesAll() For ZIP→Parquet conversion
   */
  public void downloadMetroWages() {
    LOGGER.debug("Processing QCEW bulk files for metro wages {}-{}", this.startYear, this.endYear);

    // QCEW data only available from 1990 forward
    int effectiveStartYear = Math.max(this.startYear, 1990);

    String[] frequencies = {"annual", "qtrly"};

    for (int year = effectiveStartYear; year <= this.endYear; year++) {
      for (String frequency : frequencies) {
        try {
          // Download the bulk ZIP file (method checks cache internally)
          String zipPath = downloadQcewBulkFile(year, frequency);
          LOGGER.info("Downloaded QCEW bulk {} file for year {}: {}", frequency, year, zipPath);
        } catch (IOException e) {
          LOGGER.warn("Failed to download QCEW bulk {} file for year {}: {}",
              frequency, year, e.getMessage());
          // Continue with next file instead of failing completely
        }
      }
    }

    LOGGER.info("QCEW bulk file download complete for years {}-{}", effectiveStartYear, this.endYear);
  }

  /**
   * Converts metro wage data from bulk CSV files (in ZIP archives) directly to Parquet format.
   * Uses DuckDB's zipfs extension to read CSV data without extracting or creating intermediate JSON files.
   *
   * <p>This method:
   * <ul>
   *   <li>Uses {@link #iterateTableOperationsOptimized} for efficient batch processing</li>
   *   <li>Checks cache manifest to skip already-converted years</li>
   *   <li>Uses metadata-driven CSV→Parquet conversion via {@link #convertCsvToParquet}</li>
   *   <li>Applies SQL filters to extract only the 27 major metro areas</li>
   *   <li>Maps QCEW area codes to publication codes and names via SQL CASE expressions</li>
   *   <li>Updates cache manifest after successful conversion</li>
   * </ul>
   *
   */
  public void convertMetroWagesAll() {
    LOGGER.info("Converting metro wages from CSV (ZIP) to Parquet for years {}-{}", this.startYear, this.endYear);

    String tableName = BLS.tableNames.metroWages;

    // QCEW data only available from 1990 forward
    int effectiveStartYear = Math.max(this.startYear, 1990);

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "type": return List.of(tableName);
            case "year": return yearRange(effectiveStartYear, this.endYear);
            case "frequency": return List.of("qtrly", "annual");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Use metadata-driven CSV→Parquet conversion
          convertCsvToParquet(tableName, vars);

          // Mark as converted in manifest so isTableFullyCached() works on next run
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
          LOGGER.info("Completed metro wages for year {} {}", vars.get("year"), vars.get("frequency"));
        },
        OperationType.CONVERSION);

    LOGGER.info("Metro wages conversion complete for years {}-{}", effectiveStartYear, this.endYear);
  }

  /**
   * Downloads QCEW bulk CSV file from BLS for a given year and frequency.
   * Uses metadata-driven configuration from bulkDownloads in econ-schema.json.
   *
   * <p>Downloads bulk QCEW files containing all employment/wage data in a single ZIP file.
   * This replaces individual table downloads, enabling "download once, convert many" pattern.
   *
   * <p>Cache path and URL are derived from the 'qcew_annual_bulk' bulkDownload configuration.
   *
   * <p>File sizes:
   * - Annual: ~80MB compressed → ~500MB uncompressed CSV
   * - Quarterly: ~323MB compressed → larger uncompressed CSV
   *
   * @param year Year to download (e.g., 2023)
   * @param frequency Frequency type: "annual" or "qtrly"
   * @return Path to a cached ZIP file
   * @throws IOException if download fails
   */
  String downloadQcewBulkFile(int year, String frequency) throws IOException {
    // Validate frequency parameter
    if (!"annual".equals(frequency) && !"qtrly".equals(frequency)) {
      throw new IllegalArgumentException("Frequency must be 'annual' or 'qtrly', got: " + frequency);
    }

    // Load bulkDownload configuration from schema
    Map<String, BulkDownloadConfig> bulkDownloads = loadBulkDownloads();
    BulkDownloadConfig bulkConfig = bulkDownloads.get("qcew_annual_bulk");

    if (bulkConfig == null) {
      throw new IllegalStateException("bulkDownload 'qcew_annual_bulk' not found in econ-schema.json");
    }

    // Build variables map for path/URL resolution
    Map<String, String> variables =
        ImmutableMap.of("year", String.valueOf(year),
        "frequency", frequency);

    // Resolve complete cache path from bulkDownload cachePattern
    String relativeCachePath = bulkConfig.resolveCachePath(variables);
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativeCachePath);

    // Build cache key for manifest lookup
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("year", String.valueOf(year));
    cacheParams.put("frequency", frequency);
    CacheKey cacheKey = new CacheKey("qcew_bulk", cacheParams);

    // Trust manifest first to avoid expensive S3 exists checks
    if (cacheManifest.isCached(cacheKey)) {
      LOGGER.debug("Using cached QCEW bulk {} file for {} (from manifest): {}", frequency, year, fullPath);
      return fullPath;
    }

    // Not in manifest - check file existence for self-healing
    if (cacheStorageProvider.exists(fullPath)) {
      LOGGER.info("Self-healing: Found existing QCEW bulk {} file for {}: {}", frequency, year, fullPath);
      try {
        long fileSize = cacheStorageProvider.getMetadata(fullPath).getSize();
        // Mark in manifest so future calls skip the exists check
        long refreshAfter = System.currentTimeMillis() + (365L * 24 * 60 * 60 * 1000); // 1 year for bulk files
        cacheManifest.markCached(cacheKey, relativeCachePath, fileSize, refreshAfter, "self_healed");
      } catch (Exception e) {
        LOGGER.warn("Failed to update manifest for self-healed QCEW bulk file: {}", e.getMessage());
      }
      return fullPath;
    }

    // Resolve download URL from bulkDownload url pattern
    String url = bulkConfig.resolveUrl(variables);

    LOGGER.info("Downloading QCEW bulk {} file for {} (~{}MB): {}",
                frequency, year, "annual".equals(frequency) ? "80" : "323", url);

    try {
      // Download file (blsDownloadFile() adds required User-Agent header for data.bls.gov)
      byte[] zipData = blsDownloadFile(url);

      if (zipData == null) {
        throw new IOException("Failed to download QCEW bulk file: " + url);
      }

      // Write to cache
      cacheStorageProvider.writeFile(fullPath, zipData);

      LOGGER.info("Downloaded and cached QCEW bulk {} file for {}: {} bytes",
                  frequency, year, zipData.length);

      return fullPath;

    } catch (IOException e) {
      LOGGER.error("Error downloading QCEW bulk {} file for {}: {}", frequency, year, e.getMessage());
      throw new IOException(String.format("Failed to download QCEW bulk file for year %d: %s", year, e.getMessage()), e);
    }
  }

  /**
   * Downloads JOLTS (Job Openings and Labor Turnover Survey) regional data from BLS FTP flat files.
   * Regional data is NOT available via BLS API v2 - must use download.bls.gov flat files.
   * Covers 4 Census regions (Northeast, Midwest, South, West) with 5 metrics each (20 series).
   * Uses metadata-driven FTP source paths from econ-schema.json.
   */
  public void downloadJoltsRegional() throws IOException {
    LOGGER.debug("Processing JOLTS regional data from BLS FTP flat files for {}-{}", this.startYear, this.endYear);

    String tableName = BLS.tableNames.joltsRegional;

    // Load FTP source paths from metadata
    List<FtpFileSource> ftpSources = loadFtpSourcePaths(tableName);
    if (ftpSources.isEmpty()) {
      LOGGER.warn("No FTP source paths found in metadata for table {}", tableName);
      return;
    }

    // Download all required FTP files
    LOGGER.debug("Processing {} FTP files for {}", ftpSources.size(), tableName);
    for (FtpFileSource source : ftpSources) {
      downloadFtpFileIfNeeded(source.cachePath, source.url);
    }

    iterateTableOperationsOptimized(
        tableName,
        createMetadataDimensionProvider(tableName, (dim) -> null),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          String joltsRegionalJson = parseJoltsFtpForRegional(year);

          if (joltsRegionalJson != null) {
            saveToCache(tableName, year, vars, jsonPath, joltsRegionalJson);
            LOGGER.info("Extracted {} data for year {} (4 regions × 5 metrics)", tableName, year);
          }
        },
        OperationType.DOWNLOAD);

  }

  /**
   * Downloads JOLTS state-level data from BLS FTP flat files and converts to Parquet.
   * Extracts data for all 51 states (including DC) for 5 metrics (job openings, hires, separations, quits, layoffs).
   * Uses metadata-driven FTP source paths from econ-schema.json.
   */
  public void downloadJoltsState() throws IOException {
    LOGGER.debug("Processing JOLTS state data from BLS FTP flat files for {}-{}", this.startYear, this.endYear);

    String tableName = BLS.tableNames.joltsState;

    // Load FTP source paths from metadata
    List<FtpFileSource> ftpSources = loadFtpSourcePaths(tableName);
    if (ftpSources.isEmpty()) {
      LOGGER.warn("No FTP source paths found in metadata for table {}", tableName);
      return;
    }

    // Download all required FTP files
    LOGGER.debug("Processing {} FTP files for {}", ftpSources.size(), tableName);
    for (FtpFileSource source : ftpSources) {
      downloadFtpFileIfNeeded(source.cachePath, source.url);
    }

    iterateTableOperationsOptimized(
        tableName,
        createJoltsStateDimensions(),
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Parse JOLTS FTP files for state data
          String joltsStateJson = parseJoltsFtpForState(year);

          if (joltsStateJson != null) {
            saveToCache(tableName, year, vars, jsonPath, joltsStateJson);
            LOGGER.info("Extracted {} data for year {} (51 states × 5 metrics)", tableName, year);
          }
        },
        OperationType.DOWNLOAD);

  }

  /**
   * Downloads and converts JOLTS industry reference data to Parquet.
   * Non-partitioned reference table - uses iterateTableOperationsOptimized for consistency.
   */
  public void downloadJoltsIndustries() {
    LOGGER.debug("Processing JOLTS industry reference data");

    String tableName = BLS.tableNames.referenceJoltsIndustries;

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          // Reference table only has 'type' dimension with fixed value
          if ("type".equals(dimensionName)) {
            return List.of("reference");
          }
          return null;
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Load FTP file metadata from schema
          Map<String, Object> metadata = loadTableMetadata(tableName);
          JsonNode sourcePaths = (JsonNode) metadata.get("sourcePaths");
          if (sourcePaths == null || !sourcePaths.has("ftpFiles")) {
            throw new IOException(String.format("Table %s missing sourcePaths.ftpFiles in schema", tableName));
          }

          JsonNode ftpFile = sourcePaths.get("ftpFiles").get(0);
          String ftpCachePath = ftpFile.get("cachePath").asText();
          String url = ftpFile.get("url").asText();

          // Download FTP file
          byte[] data = downloadJoltsFtpFileIfNeeded(ftpCachePath, url);

          // Parse tab-delimited file
          List<Map<String, Object>> industries = parseTabDelimitedFile(data, "industry_code", "industry_name");
          LOGGER.info("Parsed {} JOLTS industries from reference file", industries.size());

          // Convert directly to Parquet using DuckDB
          convertListToParquet(industries, parquetPath, tableName);
          LOGGER.info("Completed reference_jolts_industries");
        },
        OperationType.CONVERSION);
  }

  /**
   * Parses a tab-delimited file with two columns into a list of maps.
   * Skips the header row.
   *
   * @param data Raw file bytes
   * @param keyColumn Name for first column
   * @param valueColumn Name for second column
   * @return List of maps with keyColumn and valueColumn entries
   */
  private static List<Map<String, Object>> parseTabDelimitedFile(byte[] data, String keyColumn, String valueColumn) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (BufferedReader reader =
             new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data), StandardCharsets.UTF_8))) {
      String line;
      boolean isHeader = true;

      while ((line = reader.readLine()) != null) {
        if (isHeader) {
          isHeader = false;
          continue;
        }

        String[] fields = line.split("\\t");
        if (fields.length < 2) continue;

        Map<String, Object> row = new HashMap<>();
        row.put(keyColumn, fields[0].trim());
        row.put(valueColumn, fields[1].trim());
        rows.add(row);
      }
    }
    return rows;
  }

  /**
   * Downloads and converts JOLTS data element reference data to Parquet.
   * Non-partitioned reference table - uses iterateTableOperationsOptimized for consistency.
   */
  public void downloadJoltsDataelements() {
    LOGGER.debug("Processing JOLTS data element reference data");

    String tableName = BLS.tableNames.referenceJoltsDataelements;

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          // Reference table only has 'type' dimension with fixed value
          if ("type".equals(dimensionName)) {
            return List.of("reference");
          }
          return null;
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Load FTP file metadata from schema
          Map<String, Object> metadata = loadTableMetadata(tableName);
          JsonNode sourcePaths = (JsonNode) metadata.get("sourcePaths");
          if (sourcePaths == null || !sourcePaths.has("ftpFiles")) {
            throw new IOException(String.format("Table %s missing sourcePaths.ftpFiles in schema", tableName));
          }

          JsonNode ftpFile = sourcePaths.get("ftpFiles").get(0);
          String ftpCachePath = ftpFile.get("cachePath").asText();
          String url = ftpFile.get("url").asText();

          // Download FTP file
          byte[] data = downloadJoltsFtpFileIfNeeded(ftpCachePath, url);

          // Parse tab-delimited file
          List<Map<String, Object>> dataElements = parseTabDelimitedFile(data, "dataelement_code", "dataelement_text");
          LOGGER.info("Parsed {} JOLTS data elements from reference file", dataElements.size());

          // Convert directly to Parquet using DuckDB
          convertListToParquet(dataElements, parquetPath, tableName);
          LOGGER.info("Completed reference_jolts_dataelements");
        },
        OperationType.CONVERSION);
  }

  /**
   * Downloads inflation metrics data and converts to Parquet.
   */
  public void downloadInflationMetrics() {

    final List<String> seriesIds =
        List.of(BLS.seriesIds.inflation.cpiAllUrban,
        BLS.seriesIds.inflation.cpiCore,
        BLS.seriesIds.inflation.ppiFinalDemand);

    String tableName = BLS.tableNames.inflationMetrics;

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "type": return List.of(tableName);
            case "year": return yearRange(this.startYear, this.endYear);
            case "frequency": return List.of("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch for this year
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(tableName, seriesIds, List.of(year));
          String rawJson = resultsByYear.get(year);

          if (rawJson != null) {
            validateAndSaveBlsResponse(tableName, year, vars, jsonPath, rawJson);
          }
        },
        OperationType.DOWNLOAD);

  }

  /**
   * Downloads wage growth data and converts to Parquet.
   */
  public void downloadWageGrowth() {

    final List<String> seriesIds =
        List.of(BLS.seriesIds.wages.avgHourlyEarnings,
        BLS.seriesIds.wages.employmentCostIndex);

    String tableName = BLS.tableNames.wageGrowth;

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "type": return List.of(tableName);
            case "year": return yearRange(this.startYear, this.endYear);
            case "frequency": return List.of("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch for this year
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(tableName, seriesIds, List.of(year));
          String rawJson = resultsByYear.get(year);

          if (rawJson != null) {
            validateAndSaveBlsResponse(tableName, year, vars, jsonPath, rawJson);
          }
        },
        OperationType.DOWNLOAD);

  }

  /**
   * Downloads state-level LAUS (Local Area Unemployment Statistics) data for all 51 jurisdictions
   * (50 states + DC). Includes unemployment rate, employment level, unemployment level, and labor force.
   *
   * <p>Data is partitioned by year and state_fips, with each state saved to a separate parquet file.
   * Uses DuckDB bulk cache filtering to eliminate redundant checks across 51 states × N years.
   *
   * <p>Optimized to batch up to 20 years per API call (per state), reducing total API calls from
   * ~1,275 (51 states × 25 years) to ~102 (51 states × ~2 batches).
   */
  public void downloadRegionalEmployment() {
    LOGGER.debug("Processing regional employment data for all 51 states/jurisdictions (years {}-{})", this.startYear, this.endYear);

    String tableName = BLS.tableNames.regionalEmployment;

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "type": return List.of(tableName);
            case "year": return yearRange(this.startYear, this.endYear);
            case "state_fips": return new ArrayList<>(BLS.stateFipsCodes.values());
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));
          String stateFips = vars.get("state_fips");

          // Generate series IDs for this state (4 measures)
          // Format: LAUST{state_fips}0000000000{measure}
          // LA=Local Area, U=Unadjusted, ST=State, 08=FIPS, 0000000000=area code, 03=measure
          List<String> seriesIds = new ArrayList<>();
          seriesIds.add("LAUST" + stateFips + "0000000000003"); // unemployment rate
          seriesIds.add("LAUST" + stateFips + "0000000000004"); // unemployment level
          seriesIds.add("LAUST" + stateFips + "0000000000005"); // employment level
          seriesIds.add("LAUST" + stateFips + "0000000000006"); // labor force

          // Fetch data for this state/year
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(tableName, seriesIds, List.of(year));
          String rawJson = resultsByYear.get(year);

          if (rawJson == null) {
            LOGGER.warn("No data returned for state_fips {} year {} - skipping", stateFips, year);
            return;
          }

          // Parse and validate response
          JsonNode batchRoot = MAPPER.readTree(rawJson);
          String status = batchRoot.path("status").asText("UNKNOWN");

          if (!"REQUEST_SUCCEEDED".equals(status)) {
            LOGGER.warn("API error for state_fips {} year {}: {} - skipping", stateFips, year, status);
            return;
          }

          JsonNode seriesNode = batchRoot.path("Results").path("series");
          if (!seriesNode.isArray() || seriesNode.isEmpty()) {
            LOGGER.warn("No series data for state_fips {} year {} - skipping", stateFips, year);
            return;
          }

          // Write raw JSON to cache directory (not parquet directory)
          cacheStorageProvider.writeFile(jsonPath, rawJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));

          // Build DuckDB SQL to flatten nested JSON and apply column expressions
          // BLS returns: {Results: {series: [{seriesID: "...", data: [{year, period, value}, ...]}]}}
          // DuckDB will UNNEST the nested structure and apply expression columns
          List<PartitionedTableConfig.TableColumn> columns = loadTableColumnsFromMetadata(tableName);
          String sql = buildNestedJsonConversionSql(columns, jsonPath, parquetPath, stateFips);

          // Execute conversion
          executeDuckDBSql(sql, String.format("Regional employment conversion for state_fips %s year %d", stateFips, year));

          // Clean up temp JSON file from cache
          cacheStorageProvider.delete(jsonPath);

          LOGGER.info("Saved state_fips {} year {} ({} series)", stateFips, year, seriesNode.size());
        },
        OperationType.CONVERSION);

  }

  /**
   * Builds DuckDB SQL to convert nested BLS JSON to Parquet with column expressions.
   *
   * <p>Handles BLS API response structure:
   * <pre>{@code
   * {
   *   "Results": {
   *     "series": [
   *       {
   *         "seriesID": "LASST01...",
   *         "data": [
   *           {"year": "2020", "period": "M01", "value": "3.5"},
   *           ...
   *         ]
   *       },
   *       ...
   *     ]
   *   }
   * }
   * }</pre>
   *
   * <p>Uses DuckDB's UNNEST to flatten the nested arrays and applies column expressions
   * from the schema to transform the data.
   *
   * @param columns Schema columns with expression definitions
   * @param jsonPath Input JSON file path
   * @param parquetPath Output Parquet file path
   * @param stateFips State FIPS code to substitute in expressions
   * @return DuckDB SQL COPY statement
   */
  private String buildNestedJsonConversionSql(
      List<PartitionedTableConfig.TableColumn> columns,
      String jsonPath,
      String parquetPath,
      String stateFips) {

    // Build column expressions
    StringBuilder columnExpressions = new StringBuilder();
    boolean firstColumn = true;
    for (PartitionedTableConfig.TableColumn column : columns) {
      if (!firstColumn) {
        columnExpressions.append(",\n");
      }
      firstColumn = false;

      String columnName = column.getName();
      columnExpressions.append("    ");

      if (column.hasExpression()) {
        // Apply column expression, substituting {state_fips} placeholder
        String expression = column.getExpression().replace("{state_fips}", stateFips);

        // No parentheses needed in SELECT list - expressions already have proper structure
        // (CAST has parens, CASE has END, concatenation has clear precedence)
        columnExpressions.append(expression).append(" AS ").append(columnName);
      } else {
        // Regular column - direct mapping
        columnExpressions.append(columnName);
      }
    }

    // Load SQL template from resource file
    String sqlTemplate;
    try (InputStream is = getClass().getResourceAsStream("/sql/bls/convert_regional_employment.sql")) {
      if (is == null) {
        throw new IllegalStateException("SQL template not found: /sql/bls/convert_regional_employment.sql");
      }
      sqlTemplate = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load SQL template", e);
    }

    // Substitute parameters in template
    return sqlTemplate
        .replace("{column_expressions}", columnExpressions.toString())
        .replace("{json_path}", jsonPath)
        .replace("{parquet_path}", parquetPath);
  }

  /**
   * Downloads a file from a URL and returns the bytes.
   *
   * <p>For downloads from BLS sites (download.bls.gov and data.bls.gov), uses browser-like
   * headers to bypass bot detection. For other URLs, use a default Java HTTP client.
   *
   * @param url URL to download from
   * @return File contents as a byte array
   * @throws IOException if download fails
   */
  private byte[] blsDownloadFile(String url) throws IOException {
    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMinutes(10)) // Large files may take time
        .GET();

    // BLS sites (download.bls.gov and data.bls.gov) block default Java HTTP client
    // Add browser-like headers to bypass bot detection
    if (url.contains("download.bls.gov") || url.contains("data.bls.gov")) {
      requestBuilder
          .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
          .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
          .header("Accept-Language", "en-US,en;q=0.9");
    }

    HttpRequest request = requestBuilder.build();

    try {
      HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

      if (response.statusCode() != 200) {
        throw new IOException("HTTP request failed with status: " + response.statusCode());
      }

      return response.body();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Download interrupted", e);
    }
  }

  /**
   * Converts QCEW CSV (inside ZIP) to Parquet using DuckDB's zipfs extension.
   * Reads CSV directly from ZIP archive without extraction.
   *
   * @param fullZipPath Full path to ZIP file containing QCEW CSV
   * @param fullParquetPath Output Parquet file path
   * @param year Year (used to determine CSV filename inside ZIP)
   * @throws IOException if conversion fails
   */
  private void parseAndConvertQcewToParquet(String fullZipPath, String fullParquetPath, int year, String frequency) throws IOException {
    LOGGER.info("Converting QCEW CSV to Parquet via DuckDB zipfs for year {} frequency {}", year, frequency);

    // Determine CSV filename inside ZIP based on frequency
    // ZIP filename: YYYY_frequency_singlefile.zip (underscores)
    // CSV filename inside annual: YYYY.annual.singlefile.csv
    // CSV filename inside quarterly: YYYY.q1-q4.singlefile.csv
    String csvFilename = frequency.equals("qtrly")
        ? String.format("%d.q1-q4.singlefile.csv", year)
        : String.format("%d.annual.singlefile.csv", year);

    // Get the resource path for state_fips.json
    String stateFipsJsonPath = requireNonNull(getClass().getResource("/geo/state_fips.json")).getPath();

    // Select SQL template based on frequency (annual vs quarterly have different column schemas)
    String sqlTemplatePath = frequency.equals("qtrly")
        ? "/sql/bls/convert_county_wages_quarterly.sql"
        : "/sql/bls/convert_county_wages_annual.sql";

    // Load SQL template and substitute parameters using string replacement
    // Note: DuckDB doesn't support PreparedStatement parameters in file paths,
    // so we must use direct string substitution for read_csv_auto() and COPY TO
    String sql =
        substituteSqlParameters(loadSqlResource(sqlTemplatePath),
        ImmutableMap.of(
            "year", String.valueOf(year),
            "zipPath", fullZipPath,
            "csvFilename", csvFilename,
            "stateFipsPath", stateFipsJsonPath,
            "parquetPath", fullParquetPath));

    // Execute the SQL
    try (Connection conn = getDuckDBConnection(storageProvider);
         Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
      LOGGER.info("Successfully converted QCEW data to Parquet: {}", fullParquetPath);
    } catch (SQLException e) {
      throw new IOException("QCEW county CSV to Parquet conversion failed: " + e.getMessage(), e);
    }
  }

  /**
   * Downloads QCEW CSV file if not already cached.
   * Reuses cached data if available to avoid redundant downloads.
   *
   * @param year        Year to download
   * @param qcewZipPath Relative path for caching (from schema)
   * @param downloadUrl Download URL (from schema)
   */
  private void downloadQcewCsvIfNeeded(int year, String qcewZipPath, String downloadUrl) throws IOException {
    // Check the cache manifest first
    Map<String, String> cacheParams = ImmutableMap.of("year", String.valueOf(year));

    CacheKey cacheKey = new CacheKey("qcew_zip", cacheParams);
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);

    if (cacheManifest.isCached(cacheKey)) {
      if (cacheStorageProvider.exists(fullPath)) {
        LOGGER.info("Using cached QCEW CSV for year {} (from manifest)", year);
        return;
      } else {
        LOGGER.warn("Cache manifest lists QCEW ZIP for year {} but file not found - re-downloading", year);
      }
    } else {
      // Self-healing: cache manifest doesn't have entry, but file might exist
      if (cacheStorageProvider.exists(fullPath)) {
        LOGGER.info("Self-healing: Found QCEW ZIP for year {} in cache storage (not in manifest)", year);
        // Mark in manifest so we don't check again
        long refreshAfter = Long.MAX_VALUE;
        cacheManifest.markCached(cacheKey, qcewZipPath, -1, refreshAfter, "immutable_historical");
        return;
      }
    }

    // Download from BLS using URL from schema
    LOGGER.info("Downloading QCEW CSV for year {} from {}", year, downloadUrl);
    byte[] zipData = blsDownloadFile(downloadUrl);

    // Cache for reuse - use cacheStorageProvider for intermediate files
    cacheStorageProvider.writeFile(fullPath, zipData);

    // Mark in cache manifest - QCEW data is immutable (historical), never refresh
    long refreshAfter = Long.MAX_VALUE;
    Map<String, String> allParams = new HashMap<>(cacheParams);
    allParams.put("year", String.valueOf(year));
    CacheKey qcewCacheKey = new CacheKey("qcew_zip", allParams);
    cacheManifest.markCached(qcewCacheKey, qcewZipPath, zipData.length, refreshAfter, "immutable_historical");

    LOGGER.info("Downloaded and cached QCEW CSV for year {} ({} MB)", year, zipData.length / (1024 * 1024));

  }

  /**
   * Parses QCEW CSV (inside ZIP) and extracts state-level wage data using DuckDB's zipfs extension.
   * Filters for agglvl_code = 50 (state level), own_code = 0 (all ownership),
   * industry_code = 10 (total all industries).
   *
   * @param fullZipPath Full path to cached ZIP file containing CSV
   * @param fullParquetPath Full path for the output Parquet file
   * @param year Year of data
   * @throws IOException if conversion fails
   */
  private void parseQcewForStateWages(String fullZipPath, String fullParquetPath, int year, String frequency) throws IOException {
    LOGGER.info("Converting QCEW CSV to Parquet via DuckDB zipfs for state wages year {} frequency {}", year, frequency);

    // Determine CSV filename inside ZIP based on frequency
    // ZIP filename: YYYY_frequency_singlefile.zip (underscores)
    // CSV filename inside annual: YYYY.annual.singlefile.csv
    // CSV filename inside quarterly: YYYY.q1-q4.singlefile.csv
    String csvFilename = frequency.equals("qtrly")
        ? String.format("%d.q1-q4.singlefile.csv", year)
        : String.format("%d.annual.singlefile.csv", year);

    // Get the resource path for state_fips.json
    String stateFipsJsonPath = requireNonNull(getClass().getResource("/geo/state_fips.json")).getPath();

    // Select SQL template based on frequency (annual vs quarterly have different column schemas)
    String sqlTemplatePath = frequency.equals("qtrly")
        ? "/sql/bls/convert_state_wages_quarterly.sql"
        : "/sql/bls/convert_state_wages_annual.sql";

    // Load SQL from resource and substitute parameters using string replacement
    String sql =
        substituteSqlParameters(loadSqlResource(sqlTemplatePath),
        ImmutableMap.of(
            "year", String.valueOf(year),
            "zipPath", fullZipPath,
            "csvFilename", csvFilename,
            "stateFipsPath", stateFipsJsonPath,
            "parquetPath", fullParquetPath));

    // Execute the SQL
    try (Connection conn = getDuckDBConnection(storageProvider);
         Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
      LOGGER.info("Successfully converted state wages to Parquet: {}", fullParquetPath);
    } catch (SQLException e) {
      throw new IOException("QCEW state wages CSV to Parquet conversion failed: " + e.getMessage(), e);
    }
  }

  /**
   * Parses QCEW CSV (inside ZIP) and extracts county-level wage data using DuckDB's zipfs extension.
   * Filters for agglvl_code = 70 (county level), own_code = 0, industry_code = 10.
   * Most granular wage data available (~6,038 counties).
   *
   * @param fullZipPath Full path to cached ZIP file containing CSV
   * @param fullParquetPath Full path for the output Parquet file
   * @param year Year of data
   * @throws IOException if conversion fails
   */
  private void parseQcewForCountyWages(String fullZipPath, String fullParquetPath, int year, String frequency) throws IOException {
    LOGGER.info("Converting QCEW CSV to Parquet via DuckDB zipfs for county wages year {} frequency {}", year, frequency);

    // Determine CSV filename inside ZIP based on frequency
    // ZIP filename: YYYY_frequency_singlefile.zip (underscores)
    // CSV filename inside annual: YYYY.annual.singlefile.csv
    // CSV filename inside quarterly: YYYY.q1-q4.singlefile.csv
    String csvFilename = frequency.equals("qtrly")
        ? String.format("%d.q1-q4.singlefile.csv", year)
        : String.format("%d.annual.singlefile.csv", year);

    // Get the resource path for state_fips.json
    String stateFipsJsonPath = requireNonNull(getClass().getResource("/geo/state_fips.json")).getPath();

    // Select SQL template based on frequency (annual vs quarterly have different column schemas)
    String sqlTemplatePath = frequency.equals("qtrly")
        ? "/sql/bls/convert_county_wages_quarterly.sql"
        : "/sql/bls/convert_county_wages_annual.sql";

    // Load SQL from resource and substitute parameters using string replacement
    String sql =
        substituteSqlParameters(loadSqlResource(sqlTemplatePath),
        ImmutableMap.of(
            "year", String.valueOf(year),
            "zipPath", fullZipPath,
            "csvFilename", csvFilename,
            "stateFipsPath", stateFipsJsonPath,
            "parquetPath", fullParquetPath));

    // Execute the SQL
    try (Connection conn = getDuckDBConnection(storageProvider);
         Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
      LOGGER.info("Successfully converted county wages to Parquet: {}", fullParquetPath);
    } catch (SQLException e) {
      throw new IOException("QCEW county wages CSV to Parquet conversion failed: " + e.getMessage(), e);
    }
  }

  /**
   * Downloads a JOLTS FTP file if not already cached.
   * Returns cached data if available.
   */
  private byte[] downloadJoltsFtpFileIfNeeded(String ftpPath, String url) throws IOException {
    // Extract the file name for a cache key (e.g., "jt.series" from "type=jolts_ftp/jt.series")
    String fileName = ftpPath.substring(ftpPath.lastIndexOf('/') + 1);
    String dataType = "jolts_ftp_" + fileName.replace(".", "_");

    // Check the cache manifest first (use year=0 for non-year-partitioned files)
    Map<String, String> cacheParams =
        ImmutableMap.of("file", fileName,
        "year", String.valueOf(0));

    CacheKey cacheKey = new CacheKey(dataType, cacheParams);
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, ftpPath);

    // Trust manifest first to avoid expensive S3 exists checks
    boolean inManifest = cacheManifest.isCached(cacheKey);

    if (inManifest) {
      // Manifest says cached - try to read directly (avoids S3 exists check)
      try (InputStream inputStream = cacheStorageProvider.openInputStream(fullPath)) {
        byte[] data = inputStream.readAllBytes();
        if (data.length > 0) {
          LOGGER.debug("Using cached JOLTS FTP file: {} (from manifest, {} bytes)", ftpPath, data.length);
          return data;
        }
        LOGGER.warn("Cached JOLTS FTP file {} is zero-byte. Re-downloading.", fullPath);
      } catch (Exception e) {
        LOGGER.warn("Cache manifest lists JOLTS FTP file {} but read failed: {} - re-downloading",
            fileName, e.getMessage());
      }
    } else {
      // Not in manifest - check file existence for self-healing (S3 exists check only when needed)
      if (cacheStorageProvider.exists(fullPath)) {
        try (InputStream inputStream = cacheStorageProvider.openInputStream(fullPath)) {
          byte[] data = inputStream.readAllBytes();
          if (data.length > 0) {
            LOGGER.debug("Using cached JOLTS FTP file: {} (self-healed, {} bytes)", ftpPath, data.length);
            // Mark in manifest so future calls skip the exists check
            long refreshAfter = System.currentTimeMillis() + (30L * 24 * 60 * 60 * 1000);
            cacheManifest.markCached(cacheKey, ftpPath, data.length, refreshAfter, "self_healed");
            return data;
          }
          LOGGER.warn("Cached JOLTS FTP file {} is zero-byte. Re-downloading.", fullPath);
        } catch (Exception e) {
          LOGGER.warn("JOLTS FTP file {} exists but read failed: {} - re-downloading",
              fullPath, e.getMessage());
        }
      }
    }

    LOGGER.debug("Downloading JOLTS FTP file from {}", url);
    byte[] data = blsDownloadFile(url);

    // Cache for reuse - use cacheStorageProvider for intermediate files
    cacheStorageProvider.writeFile(fullPath, data);

    // Mark in cache manifest - refresh monthly (JOLTS data updates monthly with ~2-month lag)
    long refreshAfter = System.currentTimeMillis() + (30L * 24 * 60 * 60 * 1000); // 30 days in milliseconds
    Map<String, String> allParams = new HashMap<>(cacheParams);
    allParams.put("year", String.valueOf(0));
    CacheKey joltsFtpCacheKey = new CacheKey(dataType, allParams);
    cacheManifest.markCached(joltsFtpCacheKey, ftpPath, data.length, refreshAfter, "monthly_refresh");

    LOGGER.info("Downloaded and cached JOLTS FTP file ({} KB)", data.length / 1024);

    return data;
  }

  /**
   * Parses JOLTS FTP flat files and extracts regional data for a given year.
   * Uses metadata from schema to load FTP file paths and URLs.
   */
  private String parseJoltsFtpForRegional(int year) throws IOException {
    // Regional series patterns (state codes in positions 10-11 of series ID)
    // Use BLS region codes from constants

    // Load FTP file metadata from schema
    Map<String, Object> metadata = loadTableMetadata("jolts_regional");
    JsonNode sourcePaths = (JsonNode) metadata.get("sourcePaths");
    if (sourcePaths == null || !sourcePaths.has("ftpFiles")) {
      throw new IOException("Table jolts_regional missing sourcePaths.ftpFiles in schema");
    }

    Map<String, Map<String, Object>> regionalDataMap = new HashMap<>();

    // Process each FTP data file from schema (skip jt.series, process only jt.data.* files)
    JsonNode ftpFiles = sourcePaths.get("ftpFiles");
    int dataFilesProcessed = 0;
    for (JsonNode ftpFile : ftpFiles) {
      String cachePath = ftpFile.get("cachePath").asText();
      String url = ftpFile.get("url").asText();

      // Skip series definition file, only process data files
      if (!cachePath.contains("jt.data.")) {
        continue;
      }

      dataFilesProcessed++;

      // Extract filename for logging
      String dataFile = cachePath.substring(cachePath.lastIndexOf('/') + 1);

      byte[] data = downloadJoltsFtpFileIfNeeded(cachePath, url);

      // Parse tab-delimited file
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data), StandardCharsets.UTF_8))) {
        String line;
        boolean isHeader = true;

        int recordCount = 0;
        int debugSamples = 0;
        while ((line = reader.readLine()) != null) {
          if (isHeader) {
            isHeader = false;
            continue;
          }

          String[] fields = line.split("\\t");
          if (fields.length < 4) continue;

          String seriesId = fields[0].trim();
          String yearStr = fields[1].trim();
          String period = fields[2].trim();
          String valueStr = fields[3].trim();

          recordCount++;

          // Log the first 5 records and any matching records for debugging
          boolean m13 = yearStr.equals(String.valueOf(year)) && period.equals("M13");
          if (debugSamples < 5 || m13) {
            LOGGER.info("JOLTS Debug [{}]: seriesId={}, year={}, period={}, value={}, state_code[9-10]={}",
                debugSamples, seriesId, yearStr, period, valueStr,
                seriesId.length() >= 11 ? seriesId.substring(9, 11) : "N/A");
            if (debugSamples < 5) debugSamples++;
          }

          // Check if this is a regional series and matches our target year
          if (m13) { // Use annual average (M13)
            for (Map.Entry<String, String> entry : BLS.blsRegionCodes.entrySet()) {
              String regionCode = entry.getKey();
              String regionKey = entry.getValue();
              // Regional series: JTS000000MW00000JOR - region code at positions 10-11 (state_code field, 0-indexed substring 9-11)
              if (seriesId.length() >= 11 && seriesId.substring(9, 11).equals(regionCode)) {

                regionalDataMap.putIfAbsent(regionKey, new HashMap<>());
                Map<String, Object> regionData = regionalDataMap.get(regionKey);

                regionData.put("region", regionKey);
                regionData.put("region_code", regionCode);
                regionData.put("year", year);

                // Extract data element type from filename
                String dataElement = dataFile.replace("jt.data.", "").replaceAll("^[0-9]+\\.", "");

                try {
                  double value = Double.parseDouble(valueStr);
                  regionData.put(dataElement.toLowerCase() + "_rate", value);
                } catch (NumberFormatException e) {
                  LOGGER.warn("Failed to parse value for {} {}: {}", regionKey, dataElement, valueStr);
                }

                break;
              }
            }
          }
        }

        LOGGER.info("JOLTS file {} processed {} records", dataFile, recordCount);
      }
    }

    if (regionalDataMap.isEmpty()) {
      LOGGER.warn("No regional JOLTS data found for year {} (processed {} data files)", year, dataFilesProcessed);
      return null;
    }

    LOGGER.info("Extracted JOLTS data for {} regions (year {})", regionalDataMap.size(), year);

    try {
      return MAPPER.writeValueAsString(new ArrayList<>(regionalDataMap.values()));
    } catch (Exception e) {
      LOGGER.error("Failed to serialize JOLTS regional data to JSON: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Parses JOLTS FTP flat files and extracts state-level data for a given year.
   * Uses metadata from schema to load FTP file paths and URLs.
   * Filters for 51 states (including DC) using state codes 01-56 and converts to JSON format.
   */
  private String parseJoltsFtpForState(int year) throws IOException {
    // Load FTP file metadata from schema
    Map<String, Object> metadata = loadTableMetadata(BLS.tableNames.joltsState);
    JsonNode sourcePaths = (JsonNode) metadata.get("sourcePaths");
    if (sourcePaths == null || !sourcePaths.has("ftpFiles")) {
      throw new IOException("Table jolts_state missing sourcePaths.ftpFiles in schema");
    }

    // Map from state_code to state data
    Map<String, Map<String, Object>> stateDataMap = new HashMap<>();

    // Process each FTP data file from schema (skip jt.series, process only jt.data.* files)
    JsonNode ftpFiles = sourcePaths.get("ftpFiles");
    int dataFilesProcessed = 0;
    for (JsonNode ftpFile : ftpFiles) {
      String cachePath = ftpFile.get("cachePath").asText();
      String url = ftpFile.get("url").asText();

      // Skip series definition file, only process data files
      if (!cachePath.contains("jt.data.")) {
        continue;
      }

      dataFilesProcessed++;

      // Extract filename for logging
      String dataFile = cachePath.substring(cachePath.lastIndexOf('/') + 1);

      byte[] data = downloadJoltsFtpFileIfNeeded(cachePath, url);

      // Parse tab-delimited file
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data), StandardCharsets.UTF_8))) {
        String line;
        boolean isHeader = true;

        int recordCount = 0;
        while ((line = reader.readLine()) != null) {
          if (isHeader) {
            isHeader = false;
            continue;
          }

          String[] fields = line.split("\\t");
          if (fields.length < 4) continue;

          String seriesId = fields[0].trim();
          String yearStr = fields[1].trim();
          String period = fields[2].trim();
          String valueStr = fields[3].trim();

          recordCount++;

          // Check if this is a state series and matches our target year
          // State series: JTS00000006000000JOR - state code at positions 10-11 (state_code field, 0-indexed substring 9-11)
          // Filter for annual average (M13), total nonfarm (industry=000000), all sizes (00), level data (L)
          if (yearStr.equals(String.valueOf(year)) && period.equals("M13") && seriesId.length() >= 21) {
            String stateCode = seriesId.substring(9, 11);

            // Check if it's a state code (01-56, excluding regional codes MW, NE, SO, WE and national code 00)
            if (stateCode.matches("[0-5][0-9]") && !stateCode.equals("00")) {
              String stateName = Series.getStateName(stateCode);
              if (stateName == null) continue;

              stateDataMap.putIfAbsent(stateCode, new HashMap<>());
              Map<String, Object> stateData = stateDataMap.get(stateCode);

              stateData.put("state_fips", stateCode);
              stateData.put("state_name", stateName);
              stateData.put("year", year);

              // Extract data element type from filename
              String dataElement = dataFile.replace("jt.data.", "").replaceAll("^[0-9]+\\.", "");

              try {
                double value = Double.parseDouble(valueStr);
                stateData.put(dataElement.toLowerCase() + "_rate", value);
              } catch (NumberFormatException e) {
                LOGGER.warn("Failed to parse value for {} {}: {}", stateName, dataElement, valueStr);
              }
            }
          }
        }

        LOGGER.info("JOLTS file {} processed {} records for states", dataFile, recordCount);
      }
    }

    if (stateDataMap.isEmpty()) {
      LOGGER.warn("No state JOLTS data found for year {} (processed {} data files)", year, dataFilesProcessed);
      return null;
    }

    LOGGER.info("Extracted JOLTS data for {} states (year {})", stateDataMap.size(), year);

    try {
      return MAPPER.writeValueAsString(new ArrayList<>(stateDataMap.values()));
    } catch (Exception e) {
      LOGGER.error("Failed to serialize JOLTS state data to JSON: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Loads BLS API base URL from table metadata's download configuration.
   * @param tableName Table name to load download config from
   * @return BLS API base URL from schema metadata
   * @throws IOException If metadata cannot be loaded
   */
  private String getBlsApiUrl(String tableName) throws IOException {
    Map<String, Object> metadata = loadTableMetadata(tableName);
    JsonNode download = (JsonNode) metadata.get("download");
    if (download == null || !download.has("baseUrl")) {
      throw new IOException(String.format("Table %s does not have download.baseUrl configured", tableName));
    }
    return download.get("baseUrl").asText();
  }

  /**
   * Fetches raw JSON response for multiple BLS series in a single API call.
   * Uses the generic retry mechanism from AbstractGovDataDownloader.
   * @param tableName Table name to load API URL from schema metadata
   */
  private String fetchMultipleSeriesRaw(String tableName, List<String> seriesIds) throws IOException, InterruptedException {

    // Load API URL from table metadata
    String apiUrl = getBlsApiUrl(tableName);

    ObjectNode requestBody = MAPPER.createObjectNode();
    ArrayNode seriesArray = MAPPER.createArrayNode();
    seriesIds.forEach(seriesArray::add);
    requestBody.set("seriesid", seriesArray);
    requestBody.put("startyear", String.valueOf(this.startYear));
    requestBody.put("endyear", String.valueOf(this.endYear));
    requestBody.put("calculations", true); // Enable percent change calculations

    if (apiKey != null && !apiKey.isEmpty()) {
      requestBody.put("registrationkey", apiKey);
    }

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(apiUrl))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))
        .timeout(Duration.ofSeconds(30))
        .build();

    HttpResponse<String> response = executeWithRetry(request);

    if (response.statusCode() == 200) {
      return response.body();
    } else {
      throw new IOException("BLS API request failed with status: " + response.statusCode() +
          " - Response: " + response.body());
    }
  }

  // ===== Catalog Loading Helper Methods =====

  /**
   * Loads state FIPS codes from reference_bls_geographies catalog.
   * Uses metadata-driven pattern from schema.
   */
  private List<String> loadStateFipsFromCatalog() throws IOException {
    List<String> stateFips = new ArrayList<>();

    // Load pattern from schema metadata
    Map<String, Object> metadata = loadTableMetadata("reference_bls_geographies");
    String pattern = (String) metadata.get("pattern");

    // Resolve pattern with partition values for state geographies
    String resolvedPattern = resolveParquetPath(pattern, ImmutableMap.of("geo_type", "state"));

    String fullPath = storageProvider.resolvePath(parquetDirectory, resolvedPattern);

    try (Connection duckdb = getDuckDBConnection(storageProvider)) {
      String query =
          String.format("SELECT state_fips FROM read_parquet('%s') ORDER BY state_fips", fullPath);

      try (Statement stmt = duckdb.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          stateFips.add(rs.getString("state_fips"));
        }
      }
    } catch (Exception e) {
      throw new IOException("Failed to load state FIPS codes from catalog: " + e.getMessage(), e);
    }

    return stateFips;
  }

  /**
   * Loads census region codes from reference_bls_geographies catalog.
   * Uses metadata-driven pattern from schema.
   */
  private List<String> loadRegionCodesFromCatalog() throws IOException {
    List<String> regionCodes = new ArrayList<>();

    // Load pattern from schema metadata
    Map<String, Object> metadata = loadTableMetadata("reference_bls_geographies");
    String pattern = (String) metadata.get("pattern");

    // Resolve pattern with partition values for region geographies
    String resolvedPattern = resolveParquetPath(pattern, ImmutableMap.of("geo_type", "region"));

    String fullPath = storageProvider.resolvePath(parquetDirectory, resolvedPattern);

    try (Connection duckdb = getDuckDBConnection(storageProvider)) {
      String query =
          String.format("SELECT region_code FROM read_parquet('%s') ORDER BY region_code", fullPath);

      try (Statement stmt = duckdb.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          regionCodes.add(rs.getString("region_code"));
        }
      }
    } catch (Exception e) {
      throw new IOException("Failed to load region codes from catalog: " + e.getMessage(), e);
    }

    return regionCodes;
  }

  /**
   * Loads metro geographies from reference_bls_geographies catalog.
   * Uses metadata-driven pattern from schema.
   */
  private Map<String, MetroGeography> loadMetroGeographiesFromCatalog() throws IOException {
    Map<String, MetroGeography> metros = new HashMap<>();

    // Load pattern from schema metadata
    Map<String, Object> metadata = loadTableMetadata("reference_bls_geographies");
    String pattern = (String) metadata.get("pattern");

    // Resolve pattern with partition values for metro geographies
    String resolvedPattern = resolveParquetPath(pattern, ImmutableMap.of("geo_type", "metro"));

    String fullPath = storageProvider.resolvePath(parquetDirectory, resolvedPattern);

    try (Connection duckdb = getDuckDBConnection(storageProvider)) {
      String query =
          String.format("SELECT metro_publication_code, geo_name, metro_cpi_area_code, metro_bls_area_code "
          + "FROM read_parquet('%s') ORDER BY metro_publication_code", fullPath);

      try (Statement stmt = duckdb.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          String metroCode = rs.getString("metro_publication_code");
          String metroName = rs.getString("geo_name");
          String cpiCode = rs.getString("metro_cpi_area_code");
          String blsCode = rs.getString("metro_bls_area_code");

          metros.put(metroCode, new MetroGeography(metroCode, metroName, cpiCode, blsCode));
        }
      }
    } catch (Exception e) {
      throw new IOException("Failed to load metro geographies from catalog: " + e.getMessage(), e);
    }

    return metros;
  }

  /**
   * Loads NAICS supersector codes from reference_bls_naics_sectors catalog.
   * Uses metadata-driven pattern from schema.
   */
  private List<String> loadNaicsSectorsFromCatalog() throws IOException {
    List<String> sectors = new ArrayList<>();

    // Load pattern from schema metadata
    Map<String, Object> metadata = loadTableMetadata("reference_bls_naics_sectors");
    String pattern = (String) metadata.get("pattern");

    // No variables needed - non-partitioned reference table
    String fullPath = storageProvider.resolvePath(parquetDirectory, pattern);

    try (Connection duckdb = getDuckDBConnection(storageProvider)) {
      String query =
          String.format("SELECT supersector_code FROM read_parquet('%s') ORDER BY supersector_code", fullPath);

      try (Statement stmt = duckdb.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          sectors.add(rs.getString("supersector_code"));
        }
      }
    } catch (Exception e) {
      throw new IOException("Failed to load NAICS sectors from catalog: " + e.getMessage(), e);
    }

    return sectors;
  }

  /**
   * Override to provide BLS-specific frequency values for trend consolidation.
   * BLS tables use "monthly" instead of standard "M" abbreviation.
   */
  @Override protected List<String> getVariableValues(String tableName, String varName) {
    if ("frequency".equalsIgnoreCase(varName)) {
      // BLS tables use full word "monthly" not abbreviation "M"
      return List.of("monthly");
    }
    return super.getVariableValues(tableName, varName);
  }

  // ===== Metadata-Driven Employment Statistics Methods =====

  /**
   * Downloads BLS reference tables (JOLTS industries, dataelements, geographies, NAICS sectors).
   * Reference tables use metadata-driven approach with iterateTableOperationsOptimized.
   */
  @Override public void downloadReferenceData() throws IOException {
    LOGGER.debug("Processing BLS reference tables");

    // Download and convert JOLTS industries (now handles Parquet conversion internally)
    downloadJoltsIndustries();

    // Download and convert JOLTS data elements (now handles Parquet conversion internally)
    downloadJoltsDataelements();

    // Generate BLS geographies reference table from hardcoded maps
    generateBlsGeographiesReference();

    // Generate BLS NAICS sectors reference table from hardcoded map
    generateBlsNaicsSectorsReference();

    LOGGER.info("Completed BLS reference tables download");
  }

  /**
   * Generates BLS geographies reference table from hardcoded maps.
   * Uses metadata-driven patterns from schema for output paths.
   */
  private void generateBlsGeographiesReference() throws IOException {
    // Load pattern from schema
    Map<String, Object> metadata = loadTableMetadata("reference_bls_geographies");
    String pattern = (String) metadata.get("pattern");

    // Check if all 3 geo_type partitions are already cached
    List<String> geoTypes = Arrays.asList("state", "region", "metro");
    List<String> needsGeneration = new ArrayList<>();

    // Create FileChecker for self-healing
    DuckDBCacheStore.FileChecker fileChecker = path -> {
      try {
        if (storageProvider.exists(path)) {
          return storageProvider.getMetadata(path).getLastModified();
        }
      } catch (IOException e) {
        LOGGER.debug("Error checking file existence: {}", e.getMessage());
      }
      return -1;
    };

    for (String geoType : geoTypes) {
      Map<String, String> params = ImmutableMap.of("geo_type", geoType);
      CacheKey cacheKey = new CacheKey("reference_bls_geographies", params);
      String resolvedPattern = resolveParquetPath(pattern, params);
      String parquetPath = storageProvider.resolvePath(parquetDirectory, resolvedPattern);

      // Use centralized self-healing check
      if (((CacheManifest) cacheManifest).isParquetConvertedWithSelfHealing(
          cacheKey, parquetPath, null, fileChecker)) {
        continue;  // Already converted or self-healed
      }

      needsGeneration.add(geoType);
    }

    if (needsGeneration.isEmpty()) {
      LOGGER.debug("BLS geographies reference already cached for all geo_types, skipping");
      return;
    }

    LOGGER.info("Generating BLS geographies reference table for {} geo_types: {}",
        needsGeneration.size(), needsGeneration);

    try (Connection duckdb = getDuckDBConnection(storageProvider)) {
        // Serialize BLS constants to JSON strings for DuckDB
        String stateFipsJson = MAPPER.writeValueAsString(BLS.stateFipsCodes);
        String censusRegionsJson = MAPPER.writeValueAsString(BLS.censusRegions);
        String metroBlsAreaCodesJson = MAPPER.writeValueAsString(BLS.metroBlsAreaCodes);

        // Load SQL template and substitute JSON parameters (can't use PreparedStatement for json_each)
        String loadSql =
            substituteSqlParameters(loadSqlResource("/sql/bls/load_geographies_from_json.sql"),
            ImmutableMap.of(
                "stateFipsJson", stateFipsJson,
                "censusRegionsJson", censusRegionsJson,
                "metroBlsAreaCodesJson", metroBlsAreaCodesJson));

        // Execute SQL with direct substitution
        try (java.sql.Statement stmt = duckdb.createStatement()) {
          stmt.execute(loadSql);
        }

        // Write partitioned parquet files only for geo_types that need generation
        for (String geoType : needsGeneration) {
          // Resolve path from schema pattern
          Map<String, String> params = ImmutableMap.of("geo_type", geoType);
          String resolvedPattern = resolveParquetPath(pattern, params);
          String parquetPath = storageProvider.resolvePath(parquetDirectory, resolvedPattern);

          // Generate parquet file for this geo_type using direct substitution (COPY TO requires literal paths)
          String generateSql =
              substituteSqlParameters(loadSqlResource("/sql/bls/generate_geographies.sql"),
              ImmutableMap.of("geoType", geoType, "parquetPath", parquetPath));
          try (java.sql.Statement stmt = duckdb.createStatement()) {
            stmt.execute(generateSql);
          }

          // Count records using direct substitution
          String countSql =
              substituteSqlParameters(loadSqlResource("/sql/bls/count_geographies.sql"),
              ImmutableMap.of("geoType", geoType));
          long count = 0;
          try (java.sql.Statement stmt = duckdb.createStatement();
               ResultSet rs = stmt.executeQuery(countSql)) {
            if (rs.next()) {
              count = rs.getLong(1);
            }
          }

          LOGGER.info("Generated {} BLS {} geographies to {}", count, geoType, parquetPath);

          // Mark as converted in manifest
          CacheKey cacheKey = new CacheKey("reference_bls_geographies", params);
          cacheManifest.markParquetConverted(cacheKey, resolvedPattern);
        }
    } catch (SQLException e) {
      throw new IOException("Failed to generate BLS geographies reference table: " + e.getMessage(), e);
    }

    LOGGER.info("Completed BLS geographies reference table generation");
  }

  /**
   * Generates BLS NAICS sectors reference table from hardcoded NAICS_SUPERSECTORS map.
   * Uses metadata-driven pattern from schema for output path.
   */
  private void generateBlsNaicsSectorsReference() throws IOException {
    LOGGER.info("Generating BLS NAICS sectors reference table");

    // Load pattern from schema
    Map<String, Object> metadata = loadTableMetadata("reference_bls_naics_sectors");
    String pattern = (String) metadata.get("pattern");
    String parquetPath = storageProvider.resolvePath(parquetDirectory, pattern);

    // Check manifest with self-healing fallback
    Map<String, String> params = ImmutableMap.of();
    CacheKey cacheKey = new CacheKey("reference_bls_naics_sectors", params);

    // Create FileChecker for self-healing
    DuckDBCacheStore.FileChecker fileChecker = path -> {
      try {
        if (storageProvider.exists(path)) {
          return storageProvider.getMetadata(path).getLastModified();
        }
      } catch (IOException e) {
        LOGGER.debug("Error checking file existence: {}", e.getMessage());
      }
      return -1;
    };

    // Use centralized self-healing check
    if (((CacheManifest) cacheManifest).isParquetConvertedWithSelfHealing(
        cacheKey, parquetPath, null, fileChecker)) {
      LOGGER.debug("BLS NAICS sectors reference already cached, skipping");
      return;
    }

    try (Connection duckdb = getDuckDBConnection(storageProvider)) {
        // Serialize NAICS supersectors to JSON string for DuckDB
        String naicsJson = MAPPER.writeValueAsString(BLS.naicsSupersectors);

        // Load SQL template and substitute JSON parameter (can't use PreparedStatement for json_each)
        String loadSql =
            substituteSqlParameters(loadSqlResource("/sql/bls/load_naics_from_json.sql"),
            ImmutableMap.of("naicsSupersectorsJson", naicsJson));

        // Execute SQL with direct substitution
        try (java.sql.Statement stmt = duckdb.createStatement()) {
          stmt.execute(loadSql);
        }

        // Generate parquet file using direct substitution (COPY TO requires literal paths)
        String generateSql =
            substituteSqlParameters(loadSqlResource("/sql/bls/generate_naics_sectors.sql"),
            ImmutableMap.of("parquetPath", parquetPath));
        try (java.sql.Statement stmt = duckdb.createStatement()) {
          stmt.execute(generateSql);
        }

        // Count records
        long count = 0;
        try (java.sql.Statement stmt = duckdb.createStatement();
             ResultSet rs = stmt.executeQuery(loadSqlResource("/sql/bls/count_naics_sectors.sql"))) {
          if (rs.next()) {
            count = rs.getLong(1);
          }
        }

        LOGGER.info("Generated {} NAICS supersectors to {}", count, parquetPath);

        // Mark as converted in manifest
        cacheManifest.markParquetConverted(cacheKey, parquetPath);
    } catch (SQLException e) {
      throw new IOException("Failed to generate BLS NAICS sectors reference table: " + e.getMessage(), e);
    }

    LOGGER.info("Completed BLS NAICS sectors reference table generation");
  }
}
