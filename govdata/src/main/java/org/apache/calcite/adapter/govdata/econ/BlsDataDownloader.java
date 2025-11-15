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

  // Census region codes and names

  // Metro CPI area codes mapping: Publication Code → CPI Area Code
  // Format for CPI series: CUUR{area_code}SA0 where area_code is like S35E, S49G, etc.
  private static final Map<String, String> METRO_CPI_CODES = new HashMap<>();
  static {
    METRO_CPI_CODES.put("A100", "S35D");  // New York-Newark-Jersey City, NY-NJ-PA
    METRO_CPI_CODES.put("A400", "S49G");  // Los Angeles-Long Beach-Anaheim, CA
    METRO_CPI_CODES.put("A207", "S12A");  // Chicago-Naperville-Elgin, IL-IN-WI
    METRO_CPI_CODES.put("A425", "S37B");  // Houston-The Woodlands-Sugar Land, TX
    METRO_CPI_CODES.put("A423", "S49B");  // Phoenix-Mesa-Scottsdale, AZ
    METRO_CPI_CODES.put("A102", "S12B");  // Philadelphia-Camden-Wilmington, PA-NJ-DE-MD
    METRO_CPI_CODES.put("A426", null);    // San Antonio - No CPI data available
    METRO_CPI_CODES.put("A421", "S49E");  // San Diego-Carlsbad, CA
    METRO_CPI_CODES.put("A127", "S23A");  // Dallas-Fort Worth-Arlington, TX
    METRO_CPI_CODES.put("A429", "S49A");  // San Jose-Sunnyvale-Santa Clara, CA (San Francisco-Oakland-Hayward)
    METRO_CPI_CODES.put("A438", null);    // Austin - No CPI data available
    METRO_CPI_CODES.put("A420", "S35C");  // Jacksonville, FL (part of Miami-Fort Lauderdale)
    METRO_CPI_CODES.put("A103", "S35E");  // Boston-Cambridge-Newton, MA-NH
    METRO_CPI_CODES.put("A428", "S48B");  // Seattle-Tacoma-Bellevue, WA
    METRO_CPI_CODES.put("A427", "S48A");  // Denver-Aurora-Lakewood, CO
    METRO_CPI_CODES.put("A101", "S35B");  // Washington-Arlington-Alexandria, DC-VA-MD-WV
    METRO_CPI_CODES.put("A211", "S23B");  // Detroit-Warren-Dearborn, MI
    METRO_CPI_CODES.put("A104", null);    // Cleveland - No CPI data available
    METRO_CPI_CODES.put("A212", "S24A");  // Minneapolis-St. Paul-Bloomington, MN-WI
    METRO_CPI_CODES.put("A422", "S35C");  // Miami-Fort Lauderdale-West Palm Beach, FL
    METRO_CPI_CODES.put("A419", "S35A");  // Atlanta-Sandy Springs-Roswell, GA
    METRO_CPI_CODES.put("A437", "S49C");  // Portland-Vancouver-Hillsboro, OR-WA
    METRO_CPI_CODES.put("A424", "S49D");  // Riverside-San Bernardino-Ontario, CA
    METRO_CPI_CODES.put("A320", "S24B");  // St. Louis, MO-IL
    METRO_CPI_CODES.put("A319", null);    // Baltimore - No CPI data available
    METRO_CPI_CODES.put("A433", "S35D");  // Tampa-St. Petersburg-Clearwater, FL (shares NYC code)
    METRO_CPI_CODES.put("A440", null);    // Anchorage - No CPI data available
  }

  // Metro area codes for major metropolitan areas (Publication codes)
  private static final Map<String, String> METRO_AREA_CODES = new HashMap<>();
  static {
    METRO_AREA_CODES.put("A100", "New York-Newark-Jersey City, NY-NJ-PA");
    METRO_AREA_CODES.put("A400", "Los Angeles-Long Beach-Anaheim, CA");
    METRO_AREA_CODES.put("A207", "Chicago-Naperville-Elgin, IL-IN-WI");
    METRO_AREA_CODES.put("A425", "Houston-The Woodlands-Sugar Land, TX");
    METRO_AREA_CODES.put("A423", "Phoenix-Mesa-Scottsdale, AZ");
    METRO_AREA_CODES.put("A102", "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD");
    METRO_AREA_CODES.put("A426", "San Antonio-New Braunfels, TX");
    METRO_AREA_CODES.put("A421", "San Diego-Carlsbad, CA");
    METRO_AREA_CODES.put("A127", "Dallas-Fort Worth-Arlington, TX");
    METRO_AREA_CODES.put("A429", "San Jose-Sunnyvale-Santa Clara, CA");
    METRO_AREA_CODES.put("A438", "Austin-Round Rock, TX");
    METRO_AREA_CODES.put("A420", "Jacksonville, FL");
    METRO_AREA_CODES.put("A103", "Boston-Cambridge-Newton, MA-NH");
    METRO_AREA_CODES.put("A428", "Seattle-Tacoma-Bellevue, WA");
    METRO_AREA_CODES.put("A427", "Denver-Aurora-Lakewood, CO");
    METRO_AREA_CODES.put("A101", "Washington-Arlington-Alexandria, DC-VA-MD-WV");
    METRO_AREA_CODES.put("A211", "Detroit-Warren-Dearborn, MI");
    METRO_AREA_CODES.put("A104", "Cleveland-Elyria, OH");
    METRO_AREA_CODES.put("A212", "Minneapolis-St. Paul-Bloomington, MN-WI");
    METRO_AREA_CODES.put("A422", "Miami-Fort Lauderdale-West Palm Beach, FL");
    METRO_AREA_CODES.put("A419", "Atlanta-Sandy Springs-Roswell, GA");
    METRO_AREA_CODES.put("A437", "Portland-Vancouver-Hillsboro, OR-WA");
    METRO_AREA_CODES.put("A424", "Riverside-San Bernardino-Ontario, CA");
    METRO_AREA_CODES.put("A320", "St. Louis, MO-IL");
    METRO_AREA_CODES.put("A319", "Baltimore-Columbia-Towson, MD");
    METRO_AREA_CODES.put("A433", "Tampa-St. Petersburg-Clearwater, FL");
    METRO_AREA_CODES.put("A440", "Anchorage, AK");
  }



  // Common BLS series IDs - loaded from bls-constants.json
  public static class Series {
    // Employment Statistics
    public static final String UNEMPLOYMENT_RATE = BLS.seriesIds.employment.unemploymentRate;
    public static final String EMPLOYMENT_LEVEL = BLS.seriesIds.employment.employmentLevel;
    public static final String LABOR_FORCE_PARTICIPATION = BLS.seriesIds.employment.laborForceParticipation;

    // Inflation Metrics
    public static final String CPI_ALL_URBAN = BLS.seriesIds.inflation.cpiAllUrban;
    public static final String CPI_CORE = BLS.seriesIds.inflation.cpiCore;
    public static final String PPI_FINAL_DEMAND = BLS.seriesIds.inflation.ppiFinalDemand;

    // Wage Growth
    public static final String AVG_HOURLY_EARNINGS = BLS.seriesIds.wages.avgHourlyEarnings;
    public static final String EMPLOYMENT_COST_INDEX = BLS.seriesIds.wages.employmentCostIndex;

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
      for (String metroCode : METRO_AREA_CODES.keySet()) {
        for (String supersector : BLS.naicsSupersectors.keySet()) {
          seriesIds.add(getMetroIndustryEmploymentSeriesId(metroCode, supersector));
        }
      }
      return seriesIds;
    }

  }

  private final List<String> regionCodesList;
  private final Map<String, MetroGeography> metroGeographiesMap;

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
    public Map<String, String> metroBlsAreaCodes;

    // Industry classifications
    public Map<String, String> naicsSupersectors;

    // Series IDs
    public SeriesIds seriesIds;

    // Table names
    public TableNames tableNames;

    /**
     * Rate limiting configuration POJO.
     */
    public static class RateLimits {
      public long minRequestIntervalMs;
      public int maxRetries;
      public long retryDelayMs;
    }

    /**
     * API batching limits POJO.
     */
    public static class Batching {
      public int maxSeriesPerRequest;
      public int maxYearsPerRequest;
    }

    /**
     * Metro CPI code with metadata POJO.
     */
    public static class MetroCpiCode {
      public String cpiCode;  // null if no CPI data available
      public String name;
      public String comment;
    }

    /**
     * BLS series identifiers organized by category POJO.
     */
    public static class SeriesIds {
      public Employment employment;
      public Inflation inflation;
      public Wages wages;

      public static class Employment {
        public String unemploymentRate;
        public String employmentLevel;
        public String laborForceParticipation;
      }

      public static class Inflation {
        public String cpiAllUrban;
        public String cpiCore;
        public String ppiFinalDemand;
      }

      public static class Wages {
        public String avgHourlyEarnings;
        public String employmentCostIndex;
      }
    }

    /**
     * BLS table name constants POJO.
     */
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
      try (InputStream is = BlsConstants.class.getResourceAsStream("/bls/bls-constants.json")) {
        if (is == null) {
          throw new IOException("BLS constants resource not found: /bls/bls-constants.json");
        }
        return MAPPER.readValue(is, BlsConstants.class);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load BLS constants from /bls/bls-constants.json", e);
      }
    }
  }

  public BlsDataDownloader(String apiKey, String cacheDir, StorageProvider cacheStorageProvider, StorageProvider storageProvider) {
    this(apiKey, cacheDir, cacheDir, cacheDir, cacheStorageProvider, storageProvider, null, null);
  }

  public BlsDataDownloader(String apiKey, String cacheDir, String operatingDirectory, String parquetDirectory, StorageProvider cacheStorageProvider, StorageProvider storageProvider, CacheManifest sharedManifest, Set<String> enabledTables) {
    super(cacheDir, operatingDirectory, parquetDirectory, cacheStorageProvider, storageProvider, sharedManifest);
    this.apiKey = apiKey;
    this.enabledTables = enabledTables;

    // Load geography and sector catalogs in constructor (fail fast if missing)
    // This replaces hardcoded STATE_FIPS_MAP, CENSUS_REGIONS, METRO_*_CODES, NAICS_SUPERSECTORS
    List<String> tempStateFips;
    List<String> tempRegionCodes;
    Map<String, MetroGeography> tempMetros;
    List<String> tempNaics;

    try {
      tempStateFips = loadStateFipsFromCatalog();
      tempRegionCodes = loadRegionCodesFromCatalog();
      tempMetros = loadMetroGeographiesFromCatalog();
      tempNaics = loadNaicsSectorsFromCatalog();

      LOGGER.info("Loaded BLS catalogs: {} states, {} regions, {} metros, {} NAICS sectors",
          tempStateFips.size(), tempRegionCodes.size(), tempMetros.size(), tempNaics.size());
    } catch (Exception e) {
      LOGGER.warn("Failed to load BLS reference catalogs: {}. Will use hardcoded maps as fallback. "
          + "Run downloadReferenceData() to generate catalogs.", e.getMessage());

      // Fallback to hardcoded maps if catalogs not yet generated
      tempRegionCodes = new ArrayList<>(BLS.censusRegions.keySet());
      tempMetros = createMetroGeographiesFromHardcodedMaps();
    }

    // Catalog-loaded geography and sector lists (replaces hardcoded maps)
    this.regionCodesList = tempRegionCodes;
    this.metroGeographiesMap = tempMetros;
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
   * Batches a list of years into contiguous ranges of up to maxYears each.
   * Example: [2000, 2001, 2005, 2024, 2025] with max=20 → [[2000-2001], [2005], [2024-2025]]
   */
  private List<int[]> batchYearsIntoRanges(List<Integer> years, int maxYears) {
    if (years.isEmpty()) {
      return new ArrayList<>();
    }

    List<int[]> ranges = new ArrayList<>();
    int rangeStart = years.getFirst();
    int rangeEnd = years.getFirst();

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
        String batchJson = fetchMultipleSeriesRaw(tableName, seriesBatch, yearStart, yearEnd);
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
      return;
    }

    // Group series data by year
    Map<Integer, Map<String, ArrayNode>> dataByYear = new HashMap<>();

    for (JsonNode series : seriesArray) {
      String seriesId = series.path("seriesID").asText();
      JsonNode dataArray = series.path("data");

      if (!dataArray.isArray()) {
        continue;
      }

      // Split series data points by year
      for (JsonNode dataPoint : dataArray) {
        int year = dataPoint.path("year").asInt();
        if (year >= startYear && year <= endYear) {
          dataByYear.computeIfAbsent(year, k -> new HashMap<>())
                    .computeIfAbsent(seriesId, k -> MAPPER.createArrayNode())
                    .add(dataPoint);
        }
      }
    }

    // Create per-year JSON responses
    for (int year = startYear; year <= endYear; year++) {
      Map<String, ArrayNode> yearSeriesData = dataByYear.get(year);
      if (yearSeriesData == null || yearSeriesData.isEmpty()) {
        LOGGER.warn("No series data returned for year {} (API returned empty/no series)", year);
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

        String batchJson = fetchMultipleSeriesRaw(tableName, seriesBatch, yearStart, yearEnd);
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
    downloadAllTables(startYear, endYear, enabledTables);
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
      downloadEmploymentStatistics(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.employmentStatistics);
    }

    // Download inflation metrics
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.inflationMetrics)) {
      downloadInflationMetrics(startYear, endYear);
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
      downloadMetroWages(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.metroWages);
    }

    // Download JOLTS regional data
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.joltsRegional)) {
      downloadJoltsRegional(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.joltsRegional);
    }

    // Download JOLTS state data
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.joltsState)) {
      downloadJoltsState(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.joltsState);
    }

    // Download wage growth data
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.wageGrowth)) {
      downloadWageGrowth(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.wageGrowth);
    }

    // Download regional employment data
    if (enabledTables == null || enabledTables.contains(BLS.tableNames.regionalEmployment)) {
      downloadRegionalEmployment(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", BLS.tableNames.regionalEmployment);
    }

    // Download reference tables (always downloaded, not subject to filtering)
    LOGGER.info("Downloading JOLTS reference tables (industries, data elements)");
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
    LOGGER.info("Converting BLS data for years {}-{}", startYear, endYear);

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
          convertMetroWagesAll(startYear, endYear);
          continue;
        }

        // Other tables use JSON→Parquet conversion with DuckDB bulk cache filtering (10-20x faster)
        iterateTableOperationsOptimized(
            tableName,
            (dimensionName) -> {
              if ("year".equals(dimensionName)) {
                return yearRange(startYear, endYear);
              }
              return null; // No additional dimensions beyond year
            },
            (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
              // Add frequency variable
              Map<String, String> fullVars = new HashMap<>(vars);
              fullVars.put("frequency", "monthly");

              // Execute conversion
              convertCachedJsonToParquet(tableName, fullVars);

              // Mark as converted in manifest
              cacheManifest.markParquetConverted(cacheKey, parquetPath);
            },
            "conversion");
      }
    }

    LOGGER.info("BLS conversion complete for all enabled tables");
  }

  /**
   * Downloads employment statistics data and converts to Parquet.
   */
  public void downloadEmploymentStatistics(int startYear, int endYear) {

    // Series IDs to fetch (constant across all years)
    final List<String> seriesIds =
        List.of(Series.UNEMPLOYMENT_RATE,
        Series.EMPLOYMENT_LEVEL,
        Series.LABOR_FORCE_PARTICIPATION);

    String tableName = "employment_statistics";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "frequency": return List.of("monthly");
            default: return null;
          }
        },
        // PREFETCH CALLBACK - Batch fetch all years in ONE API call
        (context, helper) -> {
          if ("year".equals(context.segmentDimensionName)) {
            List<String> years = context.allDimensionValues.get("year");
            LOGGER.info("Prefetching employment data for {} years in single API call", years.size());

            // Convert year strings to integers
            List<Integer> yearInts = new ArrayList<>();
            for (String year : years) {
              yearInts.add(Integer.parseInt(year));
            }

            // ONE API CALL for all years
            Map<Integer, String> allData = fetchAndSplitByYear(tableName, seriesIds, yearInts);

            // Store in prefetch cache
            List<Map<String, String>> partitions = new ArrayList<>();
            List<String> jsonStrings = new ArrayList<>();
            for (Map.Entry<Integer, String> entry : allData.entrySet()) {
              partitions.add(Map.of("year", String.valueOf(entry.getKey()), "frequency", "monthly"));
              jsonStrings.add(entry.getValue());
            }
            helper.insertJsonBatch(partitions, jsonStrings);

            LOGGER.info("Prefetched {} years of employment data", allData.size());
          }
        },
        // TABLE OPERATION - Retrieve from cache
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          String rawJson = prefetchHelper.getJson(vars);

          if (rawJson != null) {
            int year = Integer.parseInt(vars.get("year"));
            String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
            validateAndSaveBlsResponse(tableName, year, vars, fullJsonPath, rawJson);
          }
        },
        "download"
    );

  }

  /**
   * Downloads CPI data for 4 Census regions (Northeast, Midwest, South, West).
   * Uses catalog-driven pattern with iterateTableOperationsOptimized() for 10-20x performance.
   */
  public void downloadRegionalCpi(int startYear, int endYear) {
    String tableName = "regional_cpi";

    LOGGER.info("Downloading regional CPI for {} Census regions for years {}-{}",
        regionCodesList.size(), startYear, endYear);

    // Build list of series IDs from catalog
    List<String> seriesIds = new ArrayList<>();
    for (String regionCode : regionCodesList) {
      seriesIds.add(Series.getRegionalCpiSeriesId(regionCode));
    }

    // Use optimized iteration with DuckDB bulk cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          if (dimensionName.equals("year")) {
            return yearRange(startYear, endYear);
          }
          return null;
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
        "download"
    );
  }

  /**
   * Downloads CPI data for major metro areas.
   * Uses catalog-driven pattern with iterateTableOperationsOptimized() for 10-20x performance.
   */
  public void downloadMetroCpi(int startYear, int endYear) {
    String tableName = "metro_cpi";

    LOGGER.info("Downloading metro area CPI for {} metros for years {}-{}",
        metroGeographiesMap.size(), startYear, endYear);

    // Build list of series IDs from catalog (only metros with CPI data)
    List<String> seriesIds = new ArrayList<>();
    for (MetroGeography metro : metroGeographiesMap.values()) {
      if (metro.cpiAreaCode != null) {
        String seriesId = "CUUR" + metro.cpiAreaCode + "SA0";
        seriesIds.add(seriesId);
      }
    }

    LOGGER.info("Found {} metros with CPI data", seriesIds.size());

    // Use optimized iteration with DuckDB bulk cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          if (dimensionName.equals("year")) {
            return yearRange(startYear, endYear);
          }
          return null;
        },
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
        "download"
    );
  }

  /**
   * Downloads employment-by-industry data for all 51 U.S. jurisdictions (50 states + DC)
   * across 22 NAICS supersector codes. Generates 1,122 series (51 × 22).
   *
   * <p>Optimized with year-batching to reduce API calls from ~345 (23 batches × 15 years)
   * to ~46 (23 batches × 2-year-batches).
   */
  public void downloadStateIndustryEmployment(int startYear, int endYear) {
    LOGGER.info("Downloading state industry employment for {} states × {} sectors ({} series) for {}-{}",
                BLS.stateFipsCodes.size(), BLS.naicsSupersectors.size(),
                BLS.stateFipsCodes.size() * BLS.naicsSupersectors.size(), startYear, endYear);

    final List<String> seriesIds = Series.getAllStateIndustryEmploymentSeriesIds();
    LOGGER.info("Generated {} state industry employment series IDs", seriesIds.size());
    LOGGER.info("State industry series examples: {}, {}, {}",
        !seriesIds.isEmpty() ? seriesIds.get(0) : "none",
                seriesIds.size() > 1 ? seriesIds.get(1) : "none",
                seriesIds.size() > 2 ? seriesIds.get(2) : "none");

    String tableName = "state_industry";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
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
        "download"
    );

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
    LOGGER.info("Downloading state wages from QCEW CSV files for {}-{}", startYear, endYear);

    String tableName = "state_wages";

    // QCEW data only available from 1990 forward
    int effectiveStartYear = Math.max(startYear, 1990);

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(effectiveStartYear, endYear);
            case "frequency": return List.of("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Get QCEW ZIP download metadata from schema
          Map<String, Object> metadata = loadTableMetadata(tableName);
          JsonNode downloadNode =
              (JsonNode) metadata.get("download");
          String cachePattern = downloadNode.get("cachePattern").asText();
          String qcewZipPath = cachePattern.replace("{year}", String.valueOf(year));
          String downloadUrl = downloadNode.get("url").asText().replace("{year}", String.valueOf(year));

          // Download QCEW CSV (reuses cache if available)
          downloadQcewCsvIfNeeded(year, qcewZipPath, downloadUrl);

          // Get full path to cached ZIP file
          String fullZipPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);

          // Parse CSV and convert to Parquet using DuckDB
          parseQcewForStateWages(fullZipPath, parquetPath, year);
          LOGGER.info("Completed state wages for year {}", year);
        },
        "convert"
    );

  }

  /**
   * Downloads county-level wage data from QCEW annual CSV files and converts to Parquet.
   * Extracts data for ~6,038 counties (most granular wage data available).
   * Reuses the same QCEW CSV files already downloaded for state wages.
   */
  public void downloadCountyWages(int startYear, int endYear) {
    LOGGER.info("Downloading county wages from QCEW CSV files for {}-{}", startYear, endYear);

    String tableName = "county_wages";

    // Get QCEW ZIP download metadata from state_wages table (shared download)
    Map<String, Object> stateWagesMetadata = loadTableMetadata("state_wages");
    JsonNode downloadNode =
        (JsonNode) stateWagesMetadata.get("download");

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "frequency": return List.of("quarterly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Get QCEW ZIP download metadata from schema
          String cachePattern = downloadNode.get("cachePattern").asText();
          String qcewZipPath = cachePattern.replace("{year}", String.valueOf(year));
          String downloadUrl = downloadNode.get("url").asText().replace("{year}", String.valueOf(year));

          // Download QCEW CSV (reuses cache from state_wages if available)
          downloadQcewCsvIfNeeded(year, qcewZipPath, downloadUrl);

          // Get full path to cached ZIP file
          String fullZipPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);

          // Parse CSV and convert to Parquet using DuckDB
          parseQcewForCountyWages(fullZipPath, parquetPath, year);
          LOGGER.info("Completed county wages for year {}", year);
        },
        "convert"
    );

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
    LOGGER.info("Downloading county QCEW data from BLS CSV files for {}-{}", startYear, endYear);

    String tableName = "county_qcew";

    // Get QCEW ZIP download metadata from state_wages table (shared download)
    Map<String, Object> stateWagesMetadata = loadTableMetadata("state_wages");
    JsonNode downloadNode =
        (JsonNode) stateWagesMetadata.get("download");

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "frequency": return List.of("quarterly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Get QCEW ZIP download metadata from schema
          String cachePattern = downloadNode.get("cachePattern").asText();
          String qcewZipPath = cachePattern.replace("{year}", String.valueOf(year));
          String downloadUrl = downloadNode.get("url").asText().replace("{year}", String.valueOf(year));

          // Download QCEW CSV (reuses cache from state_wages/county_wages if available)
          downloadQcewCsvIfNeeded(year, qcewZipPath, downloadUrl);

          // Get full path to cached ZIP file
          String fullZipPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);

          // Parse and convert to Parquet using DuckDB
          parseAndConvertQcewToParquet(fullZipPath, parquetPath, year);
          LOGGER.info("Completed county QCEW data for year {}", year);
        },
        "convert"
    );

  }

  /**
   * Downloads employment-by-industry data for 27 major U.S. metropolitan areas
   * across 22 NAICS supersector codes. Generates 594 series (27 × 22).
   *
   * <p>Optimized with year-batching to reduce API calls from ~180 (12 batches × 15 years)
   * to ~24 (12 batches × 2-year-batches).
   */
  public void downloadMetroIndustryEmployment(int startYear, int endYear) {
    LOGGER.info("Downloading metro industry employment for {} metros × {} sectors ({} series) for {}-{}",
                METRO_AREA_CODES.size(), BLS.naicsSupersectors.size(),
                METRO_AREA_CODES.size() * BLS.naicsSupersectors.size(), startYear, endYear);

    final List<String> seriesIds = Series.getAllMetroIndustryEmploymentSeriesIds();
    LOGGER.info("Generated {} metro industry employment series IDs", seriesIds.size());
    LOGGER.info("Metro industry series examples: {}, {}, {}",
        !seriesIds.isEmpty() ? seriesIds.get(0) : "none",
                seriesIds.size() > 1 ? seriesIds.get(1) : "none",
                seriesIds.size() > 2 ? seriesIds.get(2) : "none");

    String tableName = "metro_industry";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
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
        "download"
    );

  }

  /**
   * Downloads QCEW bulk ZIP files containing metro wage data.
   *
   * <p>Downloads both annual and quarterly bulk files from BLS QCEW.
   * Conversion to Parquet is handled separately by {@link #convertMetroWagesAll(int, int)}
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
   * @param startYear Start year (must be >= 1990, when QCEW data begins)
   * @param endYear   End year
   * @see #downloadQcewBulkFile(int, String) For ZIP download implementation
   * @see #convertMetroWagesAll(int, int) For ZIP→Parquet conversion
   */
  public void downloadMetroWages(int startYear, int endYear) {
    LOGGER.info("Downloading QCEW bulk files for metro wages {}-{}", startYear, endYear);

    // QCEW data only available from 1990 forward
    int effectiveStartYear = Math.max(startYear, 1990);

    String[] frequencies = {"annual", "qtrly"};

    for (int year = effectiveStartYear; year <= endYear; year++) {
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

    LOGGER.info("QCEW bulk file download complete for years {}-{}", effectiveStartYear, endYear);
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
   * @param startYear First year to convert
   * @param endYear Last year to convert
   */
  public void convertMetroWagesAll(int startYear, int endYear) {
    LOGGER.info("Converting metro wages from CSV (ZIP) to Parquet for years {}-{}", startYear, endYear);

    String tableName = BLS.tableNames.metroWages;

    // QCEW data only available from 1990 forward
    int effectiveStartYear = Math.max(startYear, 1990);

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(effectiveStartYear, endYear);
            case "frequency": return List.of("annual", "qtrly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Use metadata-driven CSV→Parquet conversion
          convertCsvToParquet(tableName, vars);
          LOGGER.info("Completed metro wages for year {} {}", vars.get("year"), vars.get("frequency"));
        },
        "convert"
    );

    LOGGER.info("Metro wages conversion complete for years {}-{}", effectiveStartYear, endYear);
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
    Map<String, String> variables = new HashMap<>();
    variables.put("year", String.valueOf(year));
    variables.put("frequency", frequency);

    // Resolve complete cache path from bulkDownload cachePattern
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory,
        bulkConfig.resolveCachePath(variables));

    // Check if file already cached
    if (cacheStorageProvider.exists(fullPath)) {
      LOGGER.info("Using cached QCEW bulk {} file for {}: {}", frequency, year, fullPath);
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
      throw new IOException("Failed to download QCEW bulk file for year " + year + ": " + e.getMessage(), e);
    }
  }

  /**
   * Downloads JOLTS (Job Openings and Labor Turnover Survey) regional data from BLS FTP flat files.
   * Regional data is NOT available via BLS API v2 - must use download.bls.gov flat files.
   * Covers 4 Census regions (Northeast, Midwest, South, West) with 5 metrics each (20 series).
   * Uses metadata-driven FTP source paths from econ-schema.json.
   */
  public void downloadJoltsRegional(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading JOLTS regional data from BLS FTP flat files for {}-{}", startYear, endYear);

    String tableName = "jolts_regional";

    // Load FTP source paths from metadata
    List<FtpFileSource> ftpSources = loadFtpSourcePaths(tableName);
    if (ftpSources.isEmpty()) {
      LOGGER.warn("No FTP source paths found in metadata for table {}", tableName);
      return;
    }

    // Download all required FTP files
    LOGGER.info("Downloading {} FTP files for {}", ftpSources.size(), tableName);
    for (FtpFileSource source : ftpSources) {
      downloadFtpFileIfNeeded(source.cachePath, source.url);
    }

    // JOLTS data only available from 2001 forward
    int effectiveStartYear = Math.max(startYear, 2001);

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(effectiveStartYear, endYear);
            case "frequency": return List.of("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          String joltsRegionalJson = parseJoltsFtpForRegional(year);

          if (joltsRegionalJson != null) {
            saveToCache(tableName, year, vars, jsonPath, joltsRegionalJson);
            LOGGER.info("Extracted {} data for year {} (4 regions × 5 metrics)", tableName, year);
          }
        },
        "download"
    );

  }

  /**
   * Downloads JOLTS state-level data from BLS FTP flat files and converts to Parquet.
   * Extracts data for all 51 states (including DC) for 5 metrics (job openings, hires, separations, quits, layoffs).
   * Uses metadata-driven FTP source paths from econ-schema.json.
   */
  public void downloadJoltsState(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading JOLTS state data from BLS FTP flat files for {}-{}", startYear, endYear);

    String tableName = "jolts_state";

    // Load FTP source paths from metadata
    List<FtpFileSource> ftpSources = loadFtpSourcePaths(tableName);
    if (ftpSources.isEmpty()) {
      LOGGER.warn("No FTP source paths found in metadata for table {}", tableName);
      return;
    }

    // Download all required FTP files
    LOGGER.info("Downloading {} FTP files for {}", ftpSources.size(), tableName);
    for (FtpFileSource source : ftpSources) {
      downloadFtpFileIfNeeded(source.cachePath, source.url);
    }

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "frequency": return List.of("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Parse JOLTS FTP files for state data
          String joltsStateJson = parseJoltsFtpForState(year);

          if (joltsStateJson != null) {
            saveToCache(tableName, year, vars, jsonPath, joltsStateJson);
            LOGGER.info("Extracted {} data for year {} (51 states × 5 metrics)", tableName, year);
          }
        },
        "download"
    );

  }

  /**
   * Downloads and converts JOLTS industry reference data to Parquet.
   * Non-partitioned reference table - uses iterateTableOperationsOptimized for consistency.
   */
  public void downloadJoltsIndustries() {
    LOGGER.info("Downloading JOLTS industry reference data from BLS FTP");

    String tableName = "reference_jolts_industries";

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
            throw new IOException("Table " + tableName + " missing sourcePaths.ftpFiles in schema");
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
        "convert"
    );
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
    LOGGER.info("Downloading JOLTS data element reference data from BLS FTP");

    String tableName = "reference_jolts_dataelements";

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
            throw new IOException("Table " + tableName + " missing sourcePaths.ftpFiles in schema");
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
        "convert"
    );
  }

  /**
   * Downloads inflation metrics data and converts to Parquet.
   */
  public void downloadInflationMetrics(int startYear, int endYear) {

    final List<String> seriesIds =
        List.of(Series.CPI_ALL_URBAN,
        Series.CPI_CORE,
        Series.PPI_FINAL_DEMAND);

    String tableName = "inflation_metrics";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
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
        "download"
    );

  }

  /**
   * Downloads wage growth data and converts to Parquet.
   */
  public void downloadWageGrowth(int startYear, int endYear) {

    final List<String> seriesIds =
        List.of(Series.AVG_HOURLY_EARNINGS,
        Series.EMPLOYMENT_COST_INDEX);

    String tableName = "wage_growth";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
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
        "download"
    );

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
  public void downloadRegionalEmployment(int startYear, int endYear) {
    LOGGER.info("Downloading regional employment data for all 51 states/jurisdictions (years {}-{})", startYear, endYear);

    String tableName = "regional_employment";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "state_fips": return new ArrayList<>(BLS.stateFipsCodes.values());
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));
          String stateFips = vars.get("state_fips");

          // Generate series IDs for this state (4 measures)
          // Format: LASST{state_fips}0000000000{measure}
          List<String> seriesIds = new ArrayList<>();
          seriesIds.add("LASST" + stateFips + "0000000000003"); // unemployment rate
          seriesIds.add("LASST" + stateFips + "0000000000004"); // unemployment level
          seriesIds.add("LASST" + stateFips + "0000000000005"); // employment level
          seriesIds.add("LASST" + stateFips + "0000000000006"); // labor force

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

          // Convert JSON response to Parquet and save
          convertAndSaveRegionalEmployment(batchRoot, parquetPath, stateFips);

          LOGGER.info("Saved state_fips {} year {} ({} series)", stateFips, year, seriesNode.size());
        },
        "convert"
    );

  }

  /**
   * Converts BLS LAUS JSON response to Parquet format and saves for a single state.
   *
   * @param jsonResponse BLS API JSON response containing series data
   * @param fullParquetPath Full path for a Parquet file (already resolved with parquet directory)
   * @param stateFips State FIPS code
   * @throws IOException if conversion or write fails
   */
  private void convertAndSaveRegionalEmployment(JsonNode jsonResponse, String fullParquetPath,
      String stateFips) throws IOException {

    // Build data records
    List<Map<String, Object>> dataRecords = new ArrayList<>();

    // Parse series from JSON response
    JsonNode seriesArray = jsonResponse.path("Results").path("series");
    if (!seriesArray.isArray()) {
      throw new IOException("Invalid BLS response: no series array found");
    }

    for (JsonNode series : seriesArray) {
      String seriesId = series.path("seriesID").asText();
      JsonNode dataArray = series.path("data");

      if (!dataArray.isArray()) {
        continue;
      }

      // Extract measure from series ID (last digit: 3=rate, 4=unemployment, 5=employment, 6=labor force)
      String measureCode = seriesId.substring(seriesId.length() - 1);
      String measure = getMeasureFromCode(measureCode);

      for (JsonNode dataPoint : dataArray) {
        String yearStr = dataPoint.path("year").asText();
        String period = dataPoint.path("period").asText();
        String valueStr = dataPoint.path("value").asText();

        // Parse value
        double value;
        try {
          value = Double.parseDouble(valueStr);
        } catch (NumberFormatException e) {
          // Skip invalid values
          continue;
        }

        // Construct date from the year and period (M01-M12 format)
        String month = period.replace("M", "");
        String date = String.format("%s-%02d-01", yearStr, Integer.parseInt(month));

        // Create a data record
        Map<String, Object> record = new HashMap<>();
        record.put("date", date);
        record.put("series_id", seriesId);
        record.put("value", value);
        record.put("area_code", stateFips);
        record.put("area_type", "state");
        record.put("measure", measure);

        dataRecords.add(record);
      }
    }

    if (dataRecords.isEmpty()) {
      LOGGER.warn("No records parsed from BLS response for state FIPS {}", stateFips);
      return;
    }

    // Load column metadata and write parquet
    List<PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("regional_employment");
    convertInMemoryToParquetViaDuckDB("regional_employment", columns, dataRecords, fullParquetPath);

    LOGGER.debug("Wrote {} records to {}", dataRecords.size(), fullParquetPath);
  }

  /**
   * Maps BLS measure code to human-readable measure name.
   */
  private String getMeasureFromCode(String code) {
    switch (code) {
      case "3":
        return "unemployment_rate";
      case "4":
        return "unemployment";
      case "5":
        return "employment";
      case "6":
        return "labor_force";
      default:
        return "unknown";
    }
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
  private void parseAndConvertQcewToParquet(String fullZipPath, String fullParquetPath, int year) throws IOException {
    LOGGER.info("Converting QCEW CSV to Parquet via DuckDB zipfs for year {}", year);

    // Determine CSV filename inside ZIP (format: YYYY.annual.singlefile.csv)
    String csvFilename = String.format("%d.annual.singlefile.csv", year);

    // Load SQL template and substitute parameters
    Map<String, String> params = new HashMap<>();
    params.put("zipPath", fullZipPath.replace("'", "''"));
    params.put("csvFilename", csvFilename.replace("'", "''"));
    params.put("parquetPath", fullParquetPath.replace("'", "''"));

    String sql = substituteSqlParameters(
        loadSqlResource("sql/qcew_county_to_parquet.sql"), params);

    // Execute via DuckDB with zipfs extension
    executeDuckDBSql(sql, "QCEW county CSV to Parquet conversion");

    LOGGER.info("Successfully converted QCEW data to Parquet: {}", fullParquetPath);
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
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("year", String.valueOf(year));

    CacheKey cacheKey = new CacheKey("qcew_zip", cacheParams);
    if (cacheManifest.isCached(cacheKey)) {
      String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);
      if (cacheStorageProvider.exists(fullPath)) {
        LOGGER.info("Using cached QCEW CSV for year {} (from manifest)", year);
        return;
      } else {
        LOGGER.warn("Cache manifest lists QCEW ZIP for year {} but file not found - re-downloading", year);
      }
    }

    // Download from BLS using URL from schema
    LOGGER.info("Downloading QCEW CSV for year {} from {}", year, downloadUrl);
    byte[] zipData = blsDownloadFile(downloadUrl);

    // Cache for reuse - use cacheStorageProvider for intermediate files
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);
    cacheStorageProvider.writeFile(fullPath, zipData);

    // Mark in cache manifest - QCEW data is immutable (historical), never refresh
    long refreshAfter = Long.MAX_VALUE;
    Map<String, String> allParams = new HashMap<>(cacheParams);
    allParams.put("year", String.valueOf(year));
    CacheKey qcewCacheKey = new CacheKey("qcew_zip", allParams);
    cacheManifest.markCached(qcewCacheKey, qcewZipPath, zipData.length, refreshAfter, "immutable_historical");
    cacheManifest.save(operatingDirectory);

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
  private void parseQcewForStateWages(String fullZipPath, String fullParquetPath, int year) throws IOException {
    LOGGER.info("Converting QCEW CSV to Parquet via DuckDB zipfs for state wages year {}", year);

    // Determine CSV filename inside ZIP
    String csvFilename = String.format("%d.annual.singlefile.csv", year);

    // Get the resource path for state_fips.json
    String stateFipsJsonPath = requireNonNull(getClass().getResource("/geo/state_fips.json")).getPath();

    // Load SQL from resource and substitute parameters
    String sql =
        substituteSqlParameters(loadSqlResource("/sql/bls/convert_state_wages.sql"),
        ImmutableMap.of(
            "year", String.valueOf(year),
            "zipPath", fullZipPath.replace("'", "''"),
            "csvFilename", csvFilename.replace("'", "''"),
            "stateFipsPath", stateFipsJsonPath.replace("'", "''"),
            "parquetPath", fullParquetPath.replace("'", "''")));

    // Execute via DuckDB with zipfs extension
    executeDuckDBSql(sql, "QCEW state wages CSV to Parquet conversion");

    LOGGER.info("Successfully converted state wages to Parquet: {}", fullParquetPath);
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
  private void parseQcewForCountyWages(String fullZipPath, String fullParquetPath, int year) throws IOException {
    LOGGER.info("Converting QCEW CSV to Parquet via DuckDB zipfs for county wages year {}", year);

    // Determine CSV filename inside ZIP
    String csvFilename = String.format("%d.annual.singlefile.csv", year);

    // Get the resource path for state_fips.json
    String stateFipsJsonPath = requireNonNull(getClass().getResource("/geo/state_fips.json")).getPath();

    // Load SQL from resource and substitute parameters
    String sql =
        substituteSqlParameters(loadSqlResource("/sql/bls/convert_county_wages.sql"),
        ImmutableMap.of(
            "year", String.valueOf(year),
            "zipPath", fullZipPath.replace("'", "''"),
            "csvFilename", csvFilename.replace("'", "''"),
            "stateFipsPath", stateFipsJsonPath.replace("'", "''"),
            "parquetPath", fullParquetPath.replace("'", "''")));

    // Execute via DuckDB with zipfs extension
    executeDuckDBSql(sql, "QCEW county wages CSV to Parquet conversion");

    LOGGER.info("Successfully converted county wages to Parquet: {}", fullParquetPath);
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
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("file", fileName);
    cacheParams.put("year", String.valueOf(0));

    CacheKey cacheKey = new CacheKey(dataType, cacheParams);
    if (cacheManifest.isCached(cacheKey)) {
      String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, ftpPath);
      if (cacheStorageProvider.exists(fullPath)) {
        long size = 0L;
        try { size = cacheStorageProvider.getMetadata(fullPath).getSize(); } catch (Exception ignore) {}
        if (size > 0) {
          LOGGER.info("Using cached JOLTS FTP file: {} (from manifest, size={} bytes)", ftpPath, size);
          try (InputStream inputStream = cacheStorageProvider.openInputStream(fullPath)) {
            return inputStream.readAllBytes();
          }
        } else {
          LOGGER.warn("Cached JOLTS FTP file {} is zero-byte (size=0). Re-downloading.", fullPath);
        }
      } else {
        LOGGER.warn("Cache manifest lists JOLTS FTP file {} but file not found - re-downloading", fileName);
      }
    }

    LOGGER.info("Downloading JOLTS FTP file from {}", url);
    byte[] data = blsDownloadFile(url);

    // Cache for reuse - use cacheStorageProvider for intermediate files
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, ftpPath);
    cacheStorageProvider.writeFile(fullPath, data);

    // Mark in cache manifest - refresh monthly (JOLTS data updates monthly with ~2-month lag)
    long refreshAfter = System.currentTimeMillis() + (30L * 24 * 60 * 60 * 1000); // 30 days in milliseconds
    Map<String, String> allParams = new HashMap<>(cacheParams);
    allParams.put("year", String.valueOf(0));
    CacheKey joltsFtpCacheKey = new CacheKey(dataType, allParams);
    cacheManifest.markCached(joltsFtpCacheKey, ftpPath, data.length, refreshAfter, "monthly_refresh");
    cacheManifest.save(operatingDirectory);

    LOGGER.info("Downloaded and cached JOLTS FTP file ({} KB)", data.length / 1024);

    return data;
  }

  /**
   * Parses JOLTS FTP flat files and extracts regional data for a given year.
   * Uses metadata from schema to load FTP file paths and URLs.
   */
  private String parseJoltsFtpForRegional(int year) throws IOException {
    // Regional series patterns (state codes in positions 10-11 of series ID)
    String[] regionCodes = {"NE", "MW", "SO", "WE"};
    String[] regionNames = {"Northeast", "Midwest", "South", "West"};

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
            for (int i = 0; i < regionCodes.length; i++) {
              // Regional series: JTS000000MW00000JOR - region code at positions 10-11 (state_code field, 0-indexed substring 9-11)
              if (seriesId.length() >= 11 && seriesId.substring(9, 11).equals(regionCodes[i])) {
                String regionKey = regionNames[i];

                regionalDataMap.putIfAbsent(regionKey, new HashMap<>());
                Map<String, Object> regionData = regionalDataMap.get(regionKey);

                regionData.put("region", regionNames[i]);
                regionData.put("region_code", regionCodes[i]);
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
    Map<String, Object> metadata = loadTableMetadata("jolts_state");
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
      throw new IOException("Table " + tableName + " does not have download.baseUrl configured");
    }
    return download.get("baseUrl").asText();
  }

  /**
   * Fetches raw JSON response for multiple BLS series in a single API call.
   * Uses the generic retry mechanism from AbstractGovDataDownloader.
   * @param tableName Table name to load API URL from schema metadata
   */
  private String fetchMultipleSeriesRaw(
      String tableName, List<String> seriesIds, int startYear, int endYear) throws IOException, InterruptedException {

    // Load API URL from table metadata
    String apiUrl = getBlsApiUrl(tableName);

    ObjectNode requestBody = MAPPER.createObjectNode();
    ArrayNode seriesArray = MAPPER.createArrayNode();
    seriesIds.forEach(seriesArray::add);
    requestBody.set("seriesid", seriesArray);
    requestBody.put("startyear", String.valueOf(startYear));
    requestBody.put("endyear", String.valueOf(endYear));
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

    try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
      String query = String.format(
          "SELECT state_fips FROM read_parquet('%s') ORDER BY state_fips", fullPath);

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

    try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
      String query = String.format(
          "SELECT region_code FROM read_parquet('%s') ORDER BY region_code", fullPath);

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

    try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
      String query = String.format(
          "SELECT metro_publication_code, geo_name, metro_cpi_area_code, metro_bls_area_code "
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

    try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
      String query = String.format(
          "SELECT supersector_code FROM read_parquet('%s') ORDER BY supersector_code", fullPath);

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
   * Creates MetroGeography map from hardcoded maps (fallback).
   */
  private Map<String, MetroGeography> createMetroGeographiesFromHardcodedMaps() {
    Map<String, MetroGeography> metros = new HashMap<>();
    for (Map.Entry<String, String> entry : METRO_AREA_CODES.entrySet()) {
      String metroCode = entry.getKey();
      String metroName = entry.getValue();
      String cpiCode = METRO_CPI_CODES.get(metroCode);
      String blsCode = BLS.metroBlsAreaCodes.get(metroCode);
      metros.put(metroCode, new MetroGeography(metroCode, metroName, cpiCode, blsCode));
    }
    return metros;
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
    LOGGER.info("Downloading BLS reference tables");

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
    LOGGER.info("Generating BLS geographies reference table");

    // Load pattern from schema
    Map<String, Object> metadata = loadTableMetadata("reference_bls_geographies");
    String pattern = (String) metadata.get("pattern");

    try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = duckdb.createStatement()) {
        // Serialize BLS constants to JSON strings for DuckDB
        String stateFipsJson = MAPPER.writeValueAsString(BLS.stateFipsCodes);
        String censusRegionsJson = MAPPER.writeValueAsString(BLS.censusRegions);
        String metroBlsAreaCodesJson = MAPPER.writeValueAsString(BLS.metroBlsAreaCodes);

        // Load and execute SQL that creates table and loads data from JSON strings
        Map<String, String> jsonParams = new HashMap<>();
        jsonParams.put("stateFipsJson", stateFipsJson.replace("'", "''"));
        jsonParams.put("censusRegionsJson", censusRegionsJson.replace("'", "''"));
        jsonParams.put("metroBlsAreaCodesJson", metroBlsAreaCodesJson.replace("'", "''"));

        String loadSql = substituteSqlParameters(
            loadSqlResource("/sql/bls/load_geographies_from_json.sql"), jsonParams);

        stmt.execute(loadSql);

        // Write partitioned parquet files by geo_type
        for (String geoType : Arrays.asList("state", "region", "metro")) {
          // Resolve path from schema pattern
          String resolvedPattern = resolveParquetPath(pattern, ImmutableMap.of("geo_type", geoType));
          String parquetPath = storageProvider.resolvePath(parquetDirectory, resolvedPattern);

          // Ensure parent directory exists
          ensureParentDirectory(parquetPath);

          // Load SQL template and substitute parameters
          Map<String, String> params = new HashMap<>();
          params.put("geoType", geoType);
          params.put("parquetPath", parquetPath.replace("'", "''"));

          String copySql = substituteSqlParameters(
              loadSqlResource("/sql/bls/generate_geographies.sql"), params);

          stmt.execute(copySql);

          long count = 0;
          Map<String, String> countParams = new HashMap<>();
          countParams.put("geoType", geoType);
          String countSql = substituteSqlParameters(
              loadSqlResource("/sql/bls/count_geographies.sql"), countParams);

          try (ResultSet rs = stmt.executeQuery(countSql)) {
            if (rs.next()) {
              count = rs.getLong(1);
            }
          }

          LOGGER.info("Generated {} BLS {} geographies to {}", count, geoType, parquetPath);
        }
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

    // Check if already exists
    Map<String, String> params = new HashMap<>();
    params.put("year", String.valueOf(-1));
    CacheKey cacheKey = new CacheKey("reference_bls_naics_sectors", params);

    if (cacheManifest.isParquetConverted(cacheKey) || storageProvider.exists(parquetPath)) {
      LOGGER.info("BLS NAICS sectors reference already exists, skipping");
      return;
    }

    try (Connection duckdb = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = duckdb.createStatement()) {
        // Serialize NAICS supersectors to JSON string for DuckDB
        String naicsJson = MAPPER.writeValueAsString(BLS.naicsSupersectors);

        // Load and execute SQL that creates table and loads data from JSON string
        Map<String, String> jsonParams = new HashMap<>();
        jsonParams.put("naicsSupersectorsJson", naicsJson.replace("'", "''"));

        String loadSql = substituteSqlParameters(
            loadSqlResource("/sql/bls/load_naics_from_json.sql"), jsonParams);

        stmt.execute(loadSql);

        // Ensure parent directory exists
        ensureParentDirectory(parquetPath);

        // Load SQL template and substitute parameters
        Map<String, String> sqlParams = new HashMap<>();
        sqlParams.put("parquetPath", parquetPath.replace("'", "''"));

        String copySql = substituteSqlParameters(
            loadSqlResource("/sql/bls/generate_naics_sectors.sql"), sqlParams);

        stmt.execute(copySql);

        long count = 0;
        String countSql = loadSqlResource("/sql/bls/count_naics_sectors.sql");

        try (ResultSet rs = stmt.executeQuery(countSql)) {
          if (rs.next()) {
            count = rs.getLong(1);
          }
        }

        LOGGER.info("Generated {} NAICS supersectors to {}", count, parquetPath);

        // Mark as converted in manifest
        cacheManifest.markParquetConverted(cacheKey, parquetPath);
        cacheManifest.save(operatingDirectory);
      }
    } catch (SQLException e) {
      throw new IOException("Failed to generate BLS NAICS sectors reference table: " + e.getMessage(), e);
    }

    LOGGER.info("Completed BLS NAICS sectors reference table generation");
  }
}
