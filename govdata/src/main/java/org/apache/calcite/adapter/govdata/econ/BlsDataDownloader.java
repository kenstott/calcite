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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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

  // State FIPS code mapping for all 50 states + DC
  private static final Map<String, String> STATE_FIPS_MAP = new HashMap<>();
  static {
    STATE_FIPS_MAP.put("AL", "01"); STATE_FIPS_MAP.put("AK", "02");
    STATE_FIPS_MAP.put("AZ", "04"); STATE_FIPS_MAP.put("AR", "05");
    STATE_FIPS_MAP.put("CA", "06"); STATE_FIPS_MAP.put("CO", "08");
    STATE_FIPS_MAP.put("CT", "09"); STATE_FIPS_MAP.put("DE", "10");
    STATE_FIPS_MAP.put("DC", "11"); STATE_FIPS_MAP.put("FL", "12");
    STATE_FIPS_MAP.put("GA", "13"); STATE_FIPS_MAP.put("HI", "15");
    STATE_FIPS_MAP.put("ID", "16"); STATE_FIPS_MAP.put("IL", "17");
    STATE_FIPS_MAP.put("IN", "18"); STATE_FIPS_MAP.put("IA", "19");
    STATE_FIPS_MAP.put("KS", "20"); STATE_FIPS_MAP.put("KY", "21");
    STATE_FIPS_MAP.put("LA", "22"); STATE_FIPS_MAP.put("ME", "23");
    STATE_FIPS_MAP.put("MD", "24"); STATE_FIPS_MAP.put("MA", "25");
    STATE_FIPS_MAP.put("MI", "26"); STATE_FIPS_MAP.put("MN", "27");
    STATE_FIPS_MAP.put("MS", "28"); STATE_FIPS_MAP.put("MO", "29");
    STATE_FIPS_MAP.put("MT", "30"); STATE_FIPS_MAP.put("NE", "31");
    STATE_FIPS_MAP.put("NV", "32"); STATE_FIPS_MAP.put("NH", "33");
    STATE_FIPS_MAP.put("NJ", "34"); STATE_FIPS_MAP.put("NM", "35");
    STATE_FIPS_MAP.put("NY", "36"); STATE_FIPS_MAP.put("NC", "37");
    STATE_FIPS_MAP.put("ND", "38"); STATE_FIPS_MAP.put("OH", "39");
    STATE_FIPS_MAP.put("OK", "40"); STATE_FIPS_MAP.put("OR", "41");
    STATE_FIPS_MAP.put("PA", "42"); STATE_FIPS_MAP.put("RI", "44");
    STATE_FIPS_MAP.put("SC", "45"); STATE_FIPS_MAP.put("SD", "46");
    STATE_FIPS_MAP.put("TN", "47"); STATE_FIPS_MAP.put("TX", "48");
    STATE_FIPS_MAP.put("UT", "49"); STATE_FIPS_MAP.put("VT", "50");
    STATE_FIPS_MAP.put("VA", "51"); STATE_FIPS_MAP.put("WA", "53");
    STATE_FIPS_MAP.put("WV", "54"); STATE_FIPS_MAP.put("WI", "55");
    STATE_FIPS_MAP.put("WY", "56");
  }

  // Census region codes and names
  private static final Map<String, String> CENSUS_REGIONS = new HashMap<>();
  static {
    CENSUS_REGIONS.put("0100", "Northeast");
    CENSUS_REGIONS.put("0200", "Midwest");
    CENSUS_REGIONS.put("0300", "South");
    CENSUS_REGIONS.put("0400", "West");
  }

  // Metro area codes for major metropolitan areas
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

  // NAICS supersector codes for industry employment
  private static final Map<String, String> NAICS_SUPERSECTORS = new HashMap<>();
  static {
    NAICS_SUPERSECTORS.put("00000000", "Total Nonfarm");
    NAICS_SUPERSECTORS.put("05000000", "Total Private");
    NAICS_SUPERSECTORS.put("06000000", "Goods Producing");
    NAICS_SUPERSECTORS.put("07000000", "Service Providing");
    NAICS_SUPERSECTORS.put("08000000", "Private Service Providing");
    NAICS_SUPERSECTORS.put("10000000", "Mining and Logging");
    NAICS_SUPERSECTORS.put("20000000", "Construction");
    NAICS_SUPERSECTORS.put("30000000", "Manufacturing");
    NAICS_SUPERSECTORS.put("31000000", "Durable Goods");
    NAICS_SUPERSECTORS.put("32000000", "Nondurable Goods");
    NAICS_SUPERSECTORS.put("40000000", "Trade, Transportation, and Utilities");
    NAICS_SUPERSECTORS.put("41000000", "Wholesale Trade");
    NAICS_SUPERSECTORS.put("42000000", "Retail Trade");
    NAICS_SUPERSECTORS.put("43000000", "Transportation and Warehousing");
    NAICS_SUPERSECTORS.put("44000000", "Utilities");
    NAICS_SUPERSECTORS.put("50000000", "Information");
    NAICS_SUPERSECTORS.put("55000000", "Financial Activities");
    NAICS_SUPERSECTORS.put("60000000", "Professional and Business Services");
    NAICS_SUPERSECTORS.put("65000000", "Education and Health Services");
    NAICS_SUPERSECTORS.put("70000000", "Leisure and Hospitality");
    NAICS_SUPERSECTORS.put("80000000", "Other Services");
    NAICS_SUPERSECTORS.put("90000000", "Government");
  }

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

    /**
     * Generates BLS regional CPI series ID.
     * Format: CUUR{REGION}SA0
     * @param regionCode 4-digit region code (e.g., "0100" for Northeast)
     */
    public static String getRegionalCpiSeriesId(String regionCode) {
      return "CUUR" + regionCode + "SA0";
    }

    /**
     * Gets all regional CPI series IDs for 4 Census regions.
     */
    public static List<String> getAllRegionalCpiSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      for (String regionCode : CENSUS_REGIONS.keySet()) {
        seriesIds.add(getRegionalCpiSeriesId(regionCode));
      }
      return seriesIds;
    }

    /**
     * Generates BLS metro area CPI series ID.
     * Format: CUURS{AREA_CODE}SA0
     * @param metroAreaCode Metro area code (e.g., "A100" for NYC)
     */
    public static String getMetroCpiSeriesId(String metroAreaCode) {
      return "CUURS" + metroAreaCode + "SA0";
    }

    /**
     * Gets all metro area CPI series IDs for 27 major metros.
     */
    public static List<String> getAllMetroCpiSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      for (String areaCode : METRO_AREA_CODES.keySet()) {
        seriesIds.add(getMetroCpiSeriesId(areaCode));
      }
      return seriesIds;
    }

    /**
     * Gets metro area name from area code.
     */
    public static String getMetroAreaName(String areaCode) {
      return METRO_AREA_CODES.getOrDefault(areaCode, "Unknown Metro");
    }

    /**
     * Generates BLS state industry employment series ID.
     * Format: SMS{STATE_FIPS}{SUPERSECTOR}
     * @param stateFips 2-digit state FIPS code (e.g., "06" for CA)
     * @param supersector 8-digit NAICS supersector code
     */
    public static String getStateIndustryEmploymentSeriesId(String stateFips, String supersector) {
      return "SMS" + stateFips + supersector;
    }

    /**
     * Gets all state industry employment series IDs.
     * Generates series for all 51 jurisdictions × 22 supersectors = 1,122 series.
     */
    public static List<String> getAllStateIndustryEmploymentSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      for (String stateFips : STATE_FIPS_MAP.values()) {
        for (String supersector : NAICS_SUPERSECTORS.keySet()) {
          seriesIds.add(getStateIndustryEmploymentSeriesId(stateFips, supersector));
        }
      }
      return seriesIds;
    }

    /**
     * Gets NAICS supersector name from code.
     */
    public static String getNaicsSupersectorName(String supersectorCode) {
      return NAICS_SUPERSECTORS.getOrDefault(supersectorCode, "Unknown Sector");
    }

    /**
     * Generates BLS state wage series ID from QCEW (Quarterly Census of Employment and Wages).
     * Format: ENU{STATE_FIPS}00010{DATA_TYPE}
     * Data types: 10 = average weekly wages, 03 = total employment
     *
     * @param stateFips 2-digit state FIPS code
     * @param dataType "10" for weekly wages, "03" for employment
     * @return QCEW series ID
     */
    public static String getStateWageSeriesId(String stateFips, String dataType) {
      return "ENU" + stateFips + "00010" + dataType;
    }

    /**
     * Gets all state average weekly wage series IDs (51 jurisdictions).
     * Uses data type "10" for average weekly wages.
     */
    public static List<String> getAllStateWageSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      for (String stateFips : STATE_FIPS_MAP.values()) {
        seriesIds.add(getStateWageSeriesId(stateFips, "10"));
      }
      return seriesIds;
    }

    /**
     * Gets state name from FIPS code.
     */
    public static String getStateName(String fipsCode) {
      for (Map.Entry<String, String> entry : STATE_FIPS_MAP.entrySet()) {
        if (entry.getValue().equals(fipsCode)) {
          return entry.getKey();
        }
      }
      return "Unknown State";
    }

    /**
     * Generates BLS metro area industry employment series ID.
     * Format: SMU{METRO_CODE}{SUPERSECTOR}
     * Example: SMUA100000000000 for NYC Total Nonfarm
     *
     * @param metroCode Metro area code (e.g., "A100" for NYC)
     * @param supersector NAICS supersector code (e.g., "00000000" for total nonfarm)
     * @return BLS metro industry employment series ID
     */
    public static String getMetroIndustryEmploymentSeriesId(String metroCode, String supersector) {
      return "SMU" + metroCode + supersector;
    }

    /**
     * Gets all metro industry employment series IDs for the 27 major metro areas.
     * Generates 594 series (27 metros × 22 sectors).
     */
    public static List<String> getAllMetroIndustryEmploymentSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      for (String metroCode : METRO_AREA_CODES.keySet()) {
        for (String supersector : NAICS_SUPERSECTORS.keySet()) {
          seriesIds.add(getMetroIndustryEmploymentSeriesId(metroCode, supersector));
        }
      }
      return seriesIds;
    }

    /**
     * Generates BLS metro area wage series ID from QCEW.
     * Format: ENUC{METRO_CODE}010
     * Data type: 10 = average weekly wages
     *
     * @param metroCode Metro area code (e.g., "A100" for NYC)
     * @return QCEW metro wage series ID
     */
    public static String getMetroWageSeriesId(String metroCode) {
      return "ENUC" + metroCode + "010";
    }

    /**
     * Gets all metro average weekly wage series IDs (27 metros).
     */
    public static List<String> getAllMetroWageSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      for (String metroCode : METRO_AREA_CODES.keySet()) {
        seriesIds.add(getMetroWageSeriesId(metroCode));
      }
      return seriesIds;
    }

    /**
     * Generates BLS JOLTS regional series ID.
     * Format: JTS{REGION}000000000{METRIC}
     *
     * @param regionCode Region code (1000=NE, 2000=MW, 3000=S, 4000=W)
     * @param metric Metric code (JOR=Job Openings Rate, HIR=Hires Rate, TSR=Total Separations,
     *               QUR=Quits Rate, LDR=Layoffs/Discharges Rate)
     * @return JOLTS series ID
     */
    public static String getJoltsRegionalSeriesId(String regionCode, String metric) {
      return "JTS" + regionCode + "000000000" + metric;
    }

    /**
     * Gets all JOLTS regional series IDs (4 regions × 5 metrics = 20 series).
     * Covers job openings, hires, total separations, quits, and layoffs/discharges
     * for Northeast, Midwest, South, and West regions.
     */
    public static List<String> getAllJoltsRegionalSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      String[] regions = {"1000", "2000", "3000", "4000"}; // NE, MW, S, W
      String[] metrics = {"JOR", "HIR", "TSR", "QUR", "LDR"}; // Openings, Hires, Separations, Quits, Layoffs

      for (String region : regions) {
        for (String metric : metrics) {
          seriesIds.add(getJoltsRegionalSeriesId(region, metric));
        }
      }
      return seriesIds;
    }
  }

  public BlsDataDownloader(String apiKey, String cacheDir, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    this(apiKey, cacheDir, cacheDir, cacheDir, cacheStorageProvider, storageProvider, null);
  }

  public BlsDataDownloader(String apiKey, String cacheDir, String operatingDirectory, String parquetDirectory, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, parquetDirectory, cacheStorageProvider, storageProvider, sharedManifest);
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

  // Table name constants for filtering
  public static final String TABLE_EMPLOYMENT_STATISTICS = "employment_statistics";
  public static final String TABLE_INFLATION_METRICS = "inflation_metrics";
  public static final String TABLE_REGIONAL_CPI = "regional_cpi";
  public static final String TABLE_METRO_CPI = "metro_cpi";
  public static final String TABLE_STATE_INDUSTRY = "state_industry";
  public static final String TABLE_STATE_WAGES = "state_wages";
  public static final String TABLE_METRO_INDUSTRY = "metro_industry";
  public static final String TABLE_METRO_WAGES = "metro_wages";
  public static final String TABLE_JOLTS_REGIONAL = "jolts_regional";
  public static final String TABLE_WAGE_GROWTH = "wage_growth";
  public static final String TABLE_REGIONAL_EMPLOYMENT = "regional_employment";

  /**
   * Downloads all BLS data for the specified year range.
   */
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
    downloadAll(startYear, endYear, null);
  }

  /**
   * Downloads BLS data for the specified year range, filtered by table names.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @param enabledTables Set of table names to download, or null to download all tables.
   *                      If provided, only tables in this set will be downloaded.
   */
  public void downloadAll(int startYear, int endYear, java.util.Set<String> enabledTables) throws IOException, InterruptedException {
    // Download employment statistics
    if (enabledTables == null || enabledTables.contains(TABLE_EMPLOYMENT_STATISTICS)) {
      downloadEmploymentStatistics(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_EMPLOYMENT_STATISTICS);
    }

    // Download inflation metrics
    if (enabledTables == null || enabledTables.contains(TABLE_INFLATION_METRICS)) {
      downloadInflationMetrics(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_INFLATION_METRICS);
    }

    // Download regional CPI
    if (enabledTables == null || enabledTables.contains(TABLE_REGIONAL_CPI)) {
      downloadRegionalCpi(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_REGIONAL_CPI);
    }

    // Download metro CPI
    if (enabledTables == null || enabledTables.contains(TABLE_METRO_CPI)) {
      downloadMetroCpi(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_METRO_CPI);
    }

    // Download state industry employment
    if (enabledTables == null || enabledTables.contains(TABLE_STATE_INDUSTRY)) {
      downloadStateIndustryEmployment(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out - saves ~1,122 series!)", TABLE_STATE_INDUSTRY);
    }

    // Download state wages
    if (enabledTables == null || enabledTables.contains(TABLE_STATE_WAGES)) {
      downloadStateWages(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_STATE_WAGES);
    }

    // Download metro industry employment
    if (enabledTables == null || enabledTables.contains(TABLE_METRO_INDUSTRY)) {
      downloadMetroIndustryEmployment(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out - saves ~594 series!)", TABLE_METRO_INDUSTRY);
    }

    // Download metro wages
    if (enabledTables == null || enabledTables.contains(TABLE_METRO_WAGES)) {
      downloadMetroWages(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_METRO_WAGES);
    }

    // Download JOLTS regional data
    if (enabledTables == null || enabledTables.contains(TABLE_JOLTS_REGIONAL)) {
      downloadJoltsRegional(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_JOLTS_REGIONAL);
    }

    // Download wage growth data
    if (enabledTables == null || enabledTables.contains(TABLE_WAGE_GROWTH)) {
      downloadWageGrowth(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_WAGE_GROWTH);
    }

    // Download regional employment data
    if (enabledTables == null || enabledTables.contains(TABLE_REGIONAL_EMPLOYMENT)) {
      downloadRegionalEmployment(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_REGIONAL_EMPLOYMENT);
    }
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
  /**
   * Downloads regional CPI using default date range from environment.
   */
  public File downloadRegionalCpi() throws IOException, InterruptedException {
    return downloadRegionalCpi(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads CPI data for 4 Census regions (Northeast, Midwest, South, West).
   */
  public File downloadRegionalCpi(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading regional CPI for 4 Census regions for {}-{}", startYear, endYear);

    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = "source=econ/type=cpi_regional/year=" + year;
      String jsonFilePath = outputDirPath + "/regional_cpi.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("regional_cpi", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached regional CPI for year {} - skipping download", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

      List<String> seriesIds = Series.getAllRegionalCpiSeriesIds();
      LOGGER.info("Generated {} regional CPI series IDs for year {}", seriesIds.size(), year);

      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);

      // Save to cache using base class helper
      saveToCache("regional_cpi", year, cacheParams, jsonFilePath, rawJson);
      lastFile = new File(jsonFilePath);
    }

    return lastFile;
  }

  /**
   * Downloads metro CPI using default date range from environment.
   */
  public File downloadMetroCpi() throws IOException, InterruptedException {
    return downloadMetroCpi(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads CPI data for 27 major metro areas.
   */
  public File downloadMetroCpi(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading metro area CPI for {} metros for {}-{}",
                METRO_AREA_CODES.size(), startYear, endYear);

    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = "source=econ/type=cpi_metro/year=" + year;
      String jsonFilePath = outputDirPath + "/metro_cpi.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("metro_cpi", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached metro CPI for year {} - skipping download", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

      List<String> seriesIds = Series.getAllMetroCpiSeriesIds();
      LOGGER.info("Generated {} metro CPI series IDs for year {}", seriesIds.size(), year);

      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);

      // Save to cache using base class helper
      saveToCache("metro_cpi", year, cacheParams, jsonFilePath, rawJson);
      lastFile = new File(jsonFilePath);
    }

    return lastFile;
  }

  public File downloadStateIndustryEmployment() throws IOException, InterruptedException {
    return downloadStateIndustryEmployment(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads employment by industry data for all 51 U.S. jurisdictions (50 states + DC)
   * across 22 NAICS supersector codes. Generates 1,122 series (51 × 22).
   */
  public File downloadStateIndustryEmployment(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading state industry employment for {} states × {} sectors ({} series) for {}-{}",
                STATE_FIPS_MAP.size(), NAICS_SUPERSECTORS.size(),
                STATE_FIPS_MAP.size() * NAICS_SUPERSECTORS.size(), startYear, endYear);

    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = "source=econ/type=state_industry/year=" + year;
      String jsonFilePath = outputDirPath + "/state_industry.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("state_industry", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached state industry employment for year {} - skipping download", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

      List<String> seriesIds = Series.getAllStateIndustryEmploymentSeriesIds();
      LOGGER.info("Generated {} state industry employment series IDs for year {}", seriesIds.size(), year);

      // Batch the series into groups of 50 to respect API limits
      ArrayNode allSeries = MAPPER.createArrayNode();

      for (int i = 0; i < seriesIds.size(); i += 50) {
        int end = Math.min(i + 50, seriesIds.size());
        List<String> batch = seriesIds.subList(i, end);

        LOGGER.info("Fetching batch {}/{} ({} series) for year {}",
                    (i / 50) + 1, (seriesIds.size() + 49) / 50, batch.size(), year);

        String batchJson = fetchMultipleSeriesRaw(batch, year, year);

        // Parse and extract series array from this batch
        JsonNode batchRoot = MAPPER.readTree(batchJson);
        JsonNode seriesNode = batchRoot.path("Results").path("series");

        if (!seriesNode.isArray()) {
          String status = batchRoot.path("status").asText("UNKNOWN");
          JsonNode messageNode = batchRoot.path("message");
          String message = messageNode.isArray() && messageNode.size() > 0
              ? messageNode.get(0).asText()
              : messageNode.asText("No error message");

          LOGGER.warn("BLS API did not return series array for batch (status: {}): {}", status, message);
          LOGGER.debug("Problematic response: {}", batchJson);

          // Check if this is a rate limit error - if so, stop immediately
          if ("REQUEST_NOT_PROCESSED".equals(status)
              && (message.contains("daily threshold") || message.contains("rate limit"))) {
            LOGGER.warn("BLS API daily rate limit reached. Stopping download for year {}.", year);
            LOGGER.info("Successfully fetched {} of {} batches before rate limit.",
                       (i / 50), (seriesIds.size() + 49) / 50);
            LOGGER.info("Partial data will be saved. Retry tomorrow after rate limit resets (midnight Eastern Time).");
            break; // Exit batch loop - save partial data
          }

          continue; // Skip this batch for other errors
        }

        // Append series to combined array
        for (JsonNode series : seriesNode) {
          allSeries.add(series);
        }

        // Rate limiting between batches
        if (end < seriesIds.size()) {
          Thread.sleep(500); // Small delay between batches
        }
      }

      // Check if we have any data to save
      if (allSeries.size() == 0) {
        LOGGER.warn("No data fetched for state industry employment year {} - skipping cache save", year);
        continue; // Skip to next year
      }

      // Build combined JSON structure
      ObjectNode combinedRoot = MAPPER.createObjectNode();
      ObjectNode resultsNode = MAPPER.createObjectNode();
      resultsNode.set("series", allSeries);
      combinedRoot.set("Results", resultsNode);
      combinedRoot.put("status", "REQUEST_SUCCEEDED"); // Mark as successful
      String rawJson = MAPPER.writeValueAsString(combinedRoot);

      // Save to cache using base class helper (partial or complete data)
      LOGGER.info("Saving state industry employment data for year {} ({} series fetched)", year, allSeries.size());
      saveToCache("state_industry", year, cacheParams, jsonFilePath, rawJson);
      lastFile = new File(jsonFilePath);
    }

    return lastFile;
  }

  public File downloadStateWages() throws IOException, InterruptedException {
    return downloadStateWages(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads average weekly wages for all 51 U.S. jurisdictions (50 states + DC)
   * from BLS QCEW (Quarterly Census of Employment and Wages). Generates 51 series.
   */
  public File downloadStateWages(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading state wages for {} jurisdictions for {}-{}",
                STATE_FIPS_MAP.size(), startYear, endYear);

    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = "source=econ/type=state_wages/year=" + year;
      String jsonFilePath = outputDirPath + "/state_wages.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("state_wages", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached state wages for year {} - skipping download", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

      List<String> seriesIds = Series.getAllStateWageSeriesIds();
      LOGGER.info("Generated {} state wage series IDs for year {}", seriesIds.size(), year);

      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);

      // Save to cache using base class helper
      saveToCache("state_wages", year, cacheParams, jsonFilePath, rawJson);
      lastFile = new File(jsonFilePath);
    }

    return lastFile;
  }

  public File downloadMetroIndustryEmployment() throws IOException, InterruptedException {
    return downloadMetroIndustryEmployment(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads employment by industry data for 27 major U.S. metropolitan areas
   * across 22 NAICS supersector codes. Generates 594 series (27 × 22).
   */
  public File downloadMetroIndustryEmployment(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading metro industry employment for {} metros × {} sectors ({} series) for {}-{}",
                METRO_AREA_CODES.size(), NAICS_SUPERSECTORS.size(),
                METRO_AREA_CODES.size() * NAICS_SUPERSECTORS.size(), startYear, endYear);

    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = "source=econ/type=metro_industry/year=" + year;
      String jsonFilePath = outputDirPath + "/metro_industry.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("metro_industry", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached metro industry employment for year {} - skipping download", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

      List<String> seriesIds = Series.getAllMetroIndustryEmploymentSeriesIds();
      LOGGER.info("Generated {} metro industry employment series IDs for year {}", seriesIds.size(), year);

      // Batch the series into groups of 50 to respect API limits
      ArrayNode allSeries = MAPPER.createArrayNode();

      for (int i = 0; i < seriesIds.size(); i += 50) {
        int end = Math.min(i + 50, seriesIds.size());
        List<String> batch = seriesIds.subList(i, end);

        LOGGER.info("Fetching batch {}/{} ({} series) for year {}",
                    (i / 50) + 1, (seriesIds.size() + 49) / 50, batch.size(), year);

        String batchJson = fetchMultipleSeriesRaw(batch, year, year);

        // Parse and extract series array from this batch
        JsonNode batchRoot = MAPPER.readTree(batchJson);
        JsonNode seriesNode = batchRoot.path("Results").path("series");

        if (!seriesNode.isArray()) {
          String status = batchRoot.path("status").asText("UNKNOWN");
          JsonNode messageNode = batchRoot.path("message");
          String message = messageNode.isArray() && messageNode.size() > 0
              ? messageNode.get(0).asText()
              : messageNode.asText("No error message");

          LOGGER.warn("BLS API did not return series array for batch (status: {}): {}", status, message);
          LOGGER.debug("Problematic response: {}", batchJson);

          // Check if this is a rate limit error - if so, stop immediately
          if ("REQUEST_NOT_PROCESSED".equals(status)
              && (message.contains("daily threshold") || message.contains("rate limit"))) {
            LOGGER.warn("BLS API daily rate limit reached. Stopping download for year {}.", year);
            LOGGER.info("Successfully fetched {} of {} batches before rate limit.",
                       (i / 50), (seriesIds.size() + 49) / 50);
            LOGGER.info("Partial data will be saved. Retry tomorrow after rate limit resets (midnight Eastern Time).");
            break; // Exit batch loop - save partial data
          }

          continue; // Skip this batch for other errors
        }

        // Append series to combined array
        for (JsonNode series : seriesNode) {
          allSeries.add(series);
        }

        // Rate limiting between batches
        if (end < seriesIds.size()) {
          Thread.sleep(500); // Small delay between batches
        }
      }

      // Check if we have any data to save
      if (allSeries.size() == 0) {
        LOGGER.warn("No data fetched for metro industry employment year {} - skipping cache save", year);
        continue; // Skip to next year
      }

      // Build combined JSON structure
      ObjectNode combinedRoot = MAPPER.createObjectNode();
      ObjectNode resultsNode = MAPPER.createObjectNode();
      resultsNode.set("series", allSeries);
      combinedRoot.set("Results", resultsNode);
      combinedRoot.put("status", "REQUEST_SUCCEEDED"); // Mark as successful
      String rawJson = MAPPER.writeValueAsString(combinedRoot);

      // Save to cache using base class helper (partial or complete data)
      LOGGER.info("Saving metro industry employment data for year {} ({} series fetched)", year, allSeries.size());
      saveToCache("metro_industry", year, cacheParams, jsonFilePath, rawJson);
      lastFile = new File(jsonFilePath);
    }

    return lastFile;
  }

  public File downloadMetroWages() throws IOException, InterruptedException {
    return downloadMetroWages(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads average weekly wages for 27 major U.S. metropolitan areas
   * from BLS QCEW (Quarterly Census of Employment and Wages). Generates 27 series.
   */
  public File downloadMetroWages(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading metro wages for {} metropolitan areas for {}-{}",
                METRO_AREA_CODES.size(), startYear, endYear);

    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = "source=econ/type=metro_wages/year=" + year;
      String jsonFilePath = outputDirPath + "/metro_wages.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("metro_wages", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached metro wages for year {} - skipping download", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

      List<String> seriesIds = Series.getAllMetroWageSeriesIds();
      LOGGER.info("Generated {} metro wage series IDs for year {}", seriesIds.size(), year);

      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);

      // Save to cache using base class helper
      saveToCache("metro_wages", year, cacheParams, jsonFilePath, rawJson);
      lastFile = new File(jsonFilePath);
    }

    return lastFile;
  }

  public File downloadJoltsRegional() throws IOException, InterruptedException {
    return downloadJoltsRegional(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads JOLTS (Job Openings and Labor Turnover Survey) data for 4 Census regions.
   * Covers job openings, hires, total separations, quits, and layoffs/discharges.
   * Generates 20 series (4 regions × 5 metrics).
   */
  public File downloadJoltsRegional(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading JOLTS regional data for 4 Census regions × 5 metrics (20 series) for {}-{}",
                startYear, endYear);

    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = "source=econ/type=jolts_regional/year=" + year;
      String jsonFilePath = outputDirPath + "/jolts_regional.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("jolts_regional", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached JOLTS regional for year {} - skipping download", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

      List<String> seriesIds = Series.getAllJoltsRegionalSeriesIds();
      LOGGER.info("Generated {} JOLTS regional series IDs for year {}", seriesIds.size(), year);

      String rawJson = fetchMultipleSeriesRaw(seriesIds, year, year);

      // Save to cache using base class helper
      saveToCache("jolts_regional", year, cacheParams, jsonFilePath, rawJson);
      lastFile = new File(jsonFilePath);
    }

    return lastFile;
  }

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
   * Downloads state-level LAUS (Local Area Unemployment Statistics) data for all 51 jurisdictions
   * (50 states + DC). Includes unemployment rate, employment level, unemployment level, and labor force.
   *
   * <p>Data is partitioned by year and state_fips, with each state saved to a separate parquet file.
   * This enables incremental downloads - if a state file already exists for a given year, it's skipped.
   */
  public File downloadRegionalEmployment(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading regional employment data for all 51 states/jurisdictions (years {}-{})", startYear, endYear);

    File lastFile = null;
    int totalStatesDownloaded = 0;
    int totalStatesSkipped = 0;

    // Download for each year separately
    for (int year = startYear; year <= endYear; year++) {
      LOGGER.info("Processing year {}: checking which states need downloading", year);

      // Check each state to see if it's already cached
      for (Map.Entry<String, String> entry : STATE_FIPS_MAP.entrySet()) {
        String stateName = entry.getKey();
        String stateFips = entry.getValue();

        // Build parquet path - relative path that gets resolved with parquetDirectory
        String relativeParquetPath = "source=econ/type=regional/year=" + year + "/state_fips=" + stateFips + "/regional_employment.parquet";
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, relativeParquetPath);

        if (storageProvider.exists(fullParquetPath)) {
          LOGGER.debug("State {} (FIPS {}) year {} already cached - skipping", stateName, stateFips, year);
          totalStatesSkipped++;
          lastFile = new File(fullParquetPath);
          continue;
        }

        LOGGER.info("Downloading state {} (FIPS {}) for year {}", stateName, stateFips, year);

        // Generate series IDs for this state
        // Format: LASST{state_fips}0000000000{measure}
        // Measures: 03=unemployment rate, 04=unemployment, 05=employment, 06=labor force
        List<String> seriesIds = new ArrayList<>();
        seriesIds.add("LASST" + stateFips + "0000000000003"); // unemployment rate
        seriesIds.add("LASST" + stateFips + "0000000000004"); // unemployment level
        seriesIds.add("LASST" + stateFips + "0000000000005"); // employment level
        seriesIds.add("LASST" + stateFips + "0000000000006"); // labor force

        try {
          // Fetch data for this state's 4 series
          String batchJson = fetchMultipleSeriesRaw(seriesIds, year, year);
          JsonNode batchRoot = MAPPER.readTree(batchJson);

          // Extract series from response
          JsonNode seriesNode = batchRoot.path("Results").path("series");

          if (!seriesNode.isArray() || seriesNode.size() == 0) {
            String status = batchRoot.path("status").asText("UNKNOWN");
            JsonNode messageNode = batchRoot.path("message");
            String message = messageNode.isArray() && messageNode.size() > 0
                ? messageNode.get(0).asText()
                : messageNode.asText("No error message");

            LOGGER.warn("Failed to fetch data for state {} (status: {}): {}", stateName, status, message);

            // Check if this is a rate limit error - if so, stop immediately
            if ("REQUEST_NOT_PROCESSED".equals(status)
                && (message.contains("daily threshold") || message.contains("rate limit"))) {
              LOGGER.warn("BLS API daily rate limit reached. Stopping download.");
              LOGGER.info("Downloaded {} states, skipped {} already cached", totalStatesDownloaded, totalStatesSkipped);
              LOGGER.info("Retry tomorrow after rate limit resets (midnight Eastern Time).");
              return lastFile;
            }

            continue; // Skip this state
          }

          // Convert JSON response to Parquet and save
          convertAndSaveRegionalEmployment(batchRoot, fullParquetPath, year, stateFips);
          totalStatesDownloaded++;
          lastFile = new File(fullParquetPath);

          LOGGER.info("Saved state {} (FIPS {}) for year {} ({} series)", stateName, stateFips, year, seriesNode.size());

        } catch (Exception e) {
          LOGGER.warn("Failed to download state {} (FIPS {}) for year {}: {}", stateName, stateFips, year, e.getMessage());
          // Continue with next state
        }

        // Rate limiting: small delay between states to avoid overwhelming API
        Thread.sleep(100);
      }
    }

    LOGGER.info("Regional employment download complete: {} states downloaded, {} already cached",
                totalStatesDownloaded, totalStatesSkipped);

    return lastFile;
  }

  /**
   * Converts BLS LAUS JSON response to Parquet format and saves for a single state.
   *
   * @param jsonResponse BLS API JSON response containing series data
   * @param fullParquetPath Full path for Parquet file (already resolved with parquet directory)
   * @param year Year of the data
   * @param stateFips State FIPS code
   * @throws IOException if conversion or write fails
   */
  private void convertAndSaveRegionalEmployment(JsonNode jsonResponse, String fullParquetPath,
      int year, String stateFips) throws IOException {

    // Define schema matching regional_employment table
    Schema schema = SchemaBuilder.record("regional_employment")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").type().stringType().noDefault()
        .name("series_id").type().stringType().noDefault()
        .name("value").type().nullable().doubleType().noDefault()
        .name("area_code").type().nullable().stringType().noDefault()
        .name("area_type").type().nullable().stringType().noDefault()
        .name("measure").type().nullable().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

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
        Double value = null;
        try {
          value = Double.parseDouble(valueStr);
        } catch (NumberFormatException e) {
          // Skip invalid values
          continue;
        }

        // Construct date from year and period (M01-M12 format)
        String month = period.replace("M", "");
        String date = String.format("%s-%02d-01", yearStr, Integer.parseInt(month));

        // Create Avro record
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", date);
        record.put("series_id", seriesId);
        record.put("value", value);
        record.put("area_code", stateFips);
        record.put("area_type", "state");
        record.put("measure", measure);

        records.add(record);
      }
    }

    if (records.isEmpty()) {
      LOGGER.warn("No records parsed from BLS response for state FIPS {}", stateFips);
      return;
    }

    // Write Parquet file - fullParquetPath is already resolved, so pass it directly
    storageProvider.writeAvroParquet(fullParquetPath, schema, records, "regional_employment");

    LOGGER.debug("Wrote {} records to {}", records.size(), fullParquetPath);
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
   * Downloads county-level LAUS (Local Area Unemployment Statistics) for all US counties.
   *
   * <p>IMPORTANT: County-level LAUS data covers approximately 3,142 US counties with 4 metrics each
   * (unemployment rate, employment level, unemployment level, labor force), totaling ~12,568 series.
   *
   * <p>API Rate Limit Considerations:
   * - BLS API supports 50 series per request
   * - ~12,568 series requires ~252 API requests
   * - With 500 requests/day limit, this takes approximately 1 day for full coverage
   * - Downloads are cached, so subsequent runs are fast
   *
   * <p>Series ID Format: LAUCN{county_5digit_fips}0000000{measure}
   * - Measures: 3=unemployment rate, 4=unemployment, 5=employment, 6=labor force
   *
   * <p>For efficiency, this method downloads all counties but batches requests and implements
   * fail-fast on rate limits. Partial data is saved, allowing resumption the next day.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @param countyFipsCodes Map of county FIPS codes to county names (from Census API)
   * @return Last file downloaded
   * @throws IOException if download fails
   * @throws InterruptedException if interrupted
   */
  public File downloadCountyEmployment(int startYear, int endYear, Map<String, String> countyFipsCodes)
      throws IOException, InterruptedException {

    if (countyFipsCodes == null || countyFipsCodes.isEmpty()) {
      LOGGER.warn("County FIPS codes not provided - cannot download county-level LAUS data. " +
          "Call CensusApiClient.getAllCountyFipsCodes() first to get county listings.");
      return null;
    }

    // Generate LAUS series IDs for all counties
    // Format: LAUCN{county_5digit_fips}0000000{measure}
    // Measures: 3=unemployment rate, 4=unemployment level, 5=employment level, 6=labor force
    List<String> allSeriesIds = new ArrayList<>();

    for (String countyFips : countyFipsCodes.keySet()) {
      // Add 4 metrics for each county
      allSeriesIds.add("LAUCN" + countyFips + "0000000003"); // unemployment rate
      allSeriesIds.add("LAUCN" + countyFips + "0000000004"); // unemployment level
      allSeriesIds.add("LAUCN" + countyFips + "0000000005"); // employment level
      allSeriesIds.add("LAUCN" + countyFips + "0000000006"); // labor force
    }

    LOGGER.info("Generated {} LAUS series IDs for {} counties",
        allSeriesIds.size(), countyFipsCodes.size());

    // Download for each year separately
    File lastFile = null;
    for (int year = startYear; year <= endYear; year++) {
      String relativePath = "source=econ/type=county/year=" + year + "/county_employment.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("county_employment", year, cacheParams, relativePath)) {
        LOGGER.info("Found cached county employment data for year {} - skipping download", year);
        lastFile = new File(relativePath);
        continue;
      }

      // BLS API supports up to 50 series per request, so batch the requests
      // We have ~12,568 series (~3,142 counties × 4 metrics), so we need ~252 batches
      ArrayNode allSeries = MAPPER.createArrayNode();

      for (int i = 0; i < allSeriesIds.size(); i += 50) {
        int endIndex = Math.min(i + 50, allSeriesIds.size());
        List<String> batch = allSeriesIds.subList(i, endIndex);

        LOGGER.info("Fetching batch {}/{} ({} series) for year {}",
                   (i / 50) + 1, (allSeriesIds.size() + 49) / 50, batch.size(), year);

        try {
          String batchJson = fetchMultipleSeriesRaw(batch, year, year);
          JsonNode batchRoot = MAPPER.readTree(batchJson);

          // Extract series from batch response
          JsonNode seriesNode = batchRoot.path("Results").path("series");

          if (!seriesNode.isArray()) {
            String status = batchRoot.path("status").asText("UNKNOWN");
            JsonNode messageNode = batchRoot.path("message");
            String message = messageNode.isArray() && messageNode.size() > 0
                ? messageNode.get(0).asText()
                : messageNode.asText("No error message");

            LOGGER.warn("BLS API did not return series array for batch (status: {}): {}", status, message);

            // Check if this is a rate limit error - if so, stop immediately
            if ("REQUEST_NOT_PROCESSED".equals(status)
                && (message.contains("daily threshold") || message.contains("rate limit"))) {
              LOGGER.warn("BLS API daily rate limit reached. Stopping download for year {}.", year);
              LOGGER.info("Successfully fetched {} of {} batches before rate limit.",
                         (i / 50), (allSeriesIds.size() + 49) / 50);
              LOGGER.info("Partial data will be saved. Retry tomorrow after rate limit resets (midnight Eastern Time).");
              break; // Exit batch loop - save partial data
            }

            continue; // Skip this batch for other errors
          }

          // Add all series from this batch to combined result
          for (JsonNode series : seriesNode) {
            allSeries.add(series);
          }

        } catch (Exception e) {
          LOGGER.warn("Failed to fetch batch {} for year {}: {}", (i / 50) + 1, year, e.getMessage());
          // Continue with other batches
        }
      }

      // Check if we have any data to save
      if (allSeries.size() == 0) {
        LOGGER.warn("No data fetched for county employment year {} - skipping cache save", year);
        continue; // Skip to next year
      }

      // Build combined JSON structure
      ObjectNode combinedRoot = MAPPER.createObjectNode();
      ObjectNode resultsNode = MAPPER.createObjectNode();
      resultsNode.set("series", allSeries);
      combinedRoot.set("Results", resultsNode);
      combinedRoot.put("status", "REQUEST_SUCCEEDED");
      String rawJson = MAPPER.writeValueAsString(combinedRoot);

      // Save to cache using base class helper
      LOGGER.info("Saving county employment data for year {} ({} series fetched)", year, allSeries.size());
      saveToCache("county_employment", year, cacheParams, relativePath, rawJson);
      lastFile = new File(relativePath);
    }

    return lastFile;
  }

  /**
   * Downloads county-level QCEW (Quarterly Census of Employment and Wages) data via flat file downloads.
   *
   * <p>IMPORTANT: County-level QCEW data with industry breakdown would generate 100,000+ series
   * via the API, which is not practical. Instead, this method downloads BLS QCEW flat files (CSV format)
   * which provide comprehensive county-level data for all industries, ownership types, and establishment sizes.
   *
   * <p>BLS QCEW Flat File Structure:
   * - Annual files: One CSV file per year with all counties, industries, and metrics
   * - Quarterly files: Separate files for each quarter
   * - URL pattern: https://data.bls.gov/cew/data/files/{year}/csv/{year}_annual_singlefile.zip
   *
   * <p>This implementation downloads annual single files which are smaller and simpler to process.
   * The files are large (~500MB zipped, ~3GB unzipped) but provide complete coverage.
   *
   * <p>TODO: Implement full CSV parsing and Parquet conversion. Current implementation downloads
   * the files but does not yet parse them. Steps needed:
   * 1. Download and unzip CSV files
   * 2. Parse CSV records (millions of rows per year)
   * 3. Filter to relevant metrics (employment, wages, establishments)
   * 4. Convert to Parquet format with proper partitioning
   * 5. Add table schema definition for query access
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @return Last file downloaded
   * @throws IOException if download fails
   * @throws InterruptedException if interrupted
   */
  public File downloadCountyWages(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading BLS QCEW flat files for county-level wage and employment data");
    LOGGER.warn("QCEW flat file parsing not yet fully implemented. Files will be downloaded but " +
        "conversion to queryable Parquet format is pending. See source code TODOs for implementation details.");

    File lastFile = null;

    for (int year = startYear; year <= endYear; year++) {
      // QCEW data only available from 1990 forward in current format
      if (year < 1990) {
        LOGGER.warn("QCEW flat files only available from 1990 forward. Skipping year {}", year);
        continue;
      }

      String relativePath = "source=econ/type=qcew/year=" + year + "/qcew_annual.zip";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("qcew_annual", year, cacheParams, relativePath)) {
        LOGGER.info("Found cached QCEW flat file for year {} - skipping download", year);
        lastFile = new File(relativePath);
        continue;
      }

      // BLS QCEW flat file URL pattern
      // Annual single file format is simplest: all counties, industries, and metrics in one CSV
      String url = String.format("https://data.bls.gov/cew/data/files/%d/csv/%d_annual_singlefile.zip",
          year, year);

      LOGGER.info("Downloading QCEW flat file for year {} from {}", year, url);

      try {
        // Download the ZIP file
        // Note: These files are large (~500MB zipped), so download may take several minutes
        byte[] zipData = downloadFile(url);

        // Save ZIP file to storage
        String zipPath = storageProvider.resolvePath(operatingDirectory, relativePath);
        storageProvider.writeFile(zipPath, zipData);

        LOGGER.info("Downloaded QCEW flat file for year {} ({} MB)",
            year, zipData.length / (1024 * 1024));

        // Parse CSV and convert to Parquet
        String relativeParquetPath = "source=econ/type=county_qcew/year=" + year + "/county_qcew.parquet";
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, relativeParquetPath);
        parseAndConvertQcewToParquet(zipData, fullParquetPath, year);

        // Save cache metadata
        saveToCache("qcew_annual", year, cacheParams, relativePath, "");
        lastFile = new File(relativePath);

      } catch (Exception e) {
        LOGGER.error("Failed to download QCEW flat file for year {}: {}", year, e.getMessage());
        // Continue with other years
      }
    }

    return lastFile;
  }

  /**
   * Downloads a file from a URL and returns the bytes.
   *
   * @param url URL to download from
   * @return File contents as byte array
   * @throws IOException if download fails
   */
  private byte[] downloadFile(String url) throws IOException {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMinutes(10)) // Large files may take time
        .GET()
        .build();

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
   * Parses QCEW flat file (ZIP containing CSV) and converts to Parquet format.
   *
   * <p>Extracts county-level records from the QCEW annual CSV file and converts to Parquet.
   * Filters to county-level aggregation (agglvl_code 70-78) to focus on county-specific data.
   *
   * <p>Schema includes:
   * - area_fips: 5-digit county FIPS code
   * - own_code: Ownership code (0=Total, 1-5=various types)
   * - industry_code: 6-character NAICS industry code
   * - agglvl_code: Aggregation level (70-78 for county data)
   * - annual_avg_estabs: Annual average establishment count
   * - annual_avg_emplvl: Annual average employment level
   * - total_annual_wages: Total annual wages
   * - annual_avg_wkly_wage: Average weekly wage
   *
   * @param zipData Downloaded ZIP file bytes
   * @param fullParquetPath Full path for output Parquet file (already resolved)
   * @param year Year of the data
   * @throws IOException if parsing or conversion fails
   */
  private void parseAndConvertQcewToParquet(byte[] zipData, String fullParquetPath, int year) throws IOException {
    LOGGER.info("Parsing QCEW CSV for year {} and converting to Parquet", year);

    // Define Avro schema for QCEW county data
    Schema schema = SchemaBuilder.record("CountyQcew")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("area_fips").type().stringType().noDefault()
        .name("own_code").type().stringType().noDefault()
        .name("industry_code").type().stringType().noDefault()
        .name("agglvl_code").type().stringType().noDefault()
        .name("annual_avg_estabs").type().nullable().intType().noDefault()
        .name("annual_avg_emplvl").type().nullable().intType().noDefault()
        .name("total_annual_wages").type().nullable().longType().noDefault()
        .name("annual_avg_wkly_wage").type().nullable().intType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    int recordCount = 0;
    int countyRecordCount = 0;

    // Unzip and parse CSV
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipData))) {
      ZipEntry entry;

      while ((entry = zis.getNextEntry()) != null) {
        // Look for CSV files in the ZIP
        if (entry.getName().endsWith(".csv")) {
          LOGGER.info("Processing CSV file: {}", entry.getName());

          try (BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8))) {
            String line;
            boolean isHeader = true;

            while ((line = reader.readLine()) != null) {
              // Skip header row
              if (isHeader) {
                isHeader = false;
                continue;
              }

              recordCount++;

              // Parse CSV line (QCEW uses comma-separated values with quotes)
              String[] fields = parseCsvLine(line);

              if (fields.length < 20) {
                continue; // Skip malformed records
              }

              try {
                String areaFips = fields[0].trim();
                String ownCode = fields[1].trim();
                String industryCode = fields[2].trim();
                String agglvlCode = fields[3].trim();

                // Filter to county-level data only (agglvl 70-78 are county aggregations)
                // 70 = County, Total
                // 71-78 = Various county-level industry aggregations
                if (!agglvlCode.startsWith("7")) {
                  continue;
                }

                // Filter to actual counties (5-digit FIPS codes, not state or national)
                if (areaFips.length() != 5 || areaFips.equals("US000")) {
                  continue;
                }

                countyRecordCount++;

                // Parse numeric fields (handle empty values)
                Integer avgEstabs = parseIntOrNull(fields[13]);
                Integer avgEmplvl = parseIntOrNull(fields[14]);
                Long totalWages = parseLongOrNull(fields[15]);
                Integer avgWklyWage = parseIntOrNull(fields[18]);

                // Create Avro record
                GenericRecord record = new GenericData.Record(schema);
                record.put("area_fips", areaFips);
                record.put("own_code", ownCode);
                record.put("industry_code", industryCode);
                record.put("agglvl_code", agglvlCode);
                record.put("annual_avg_estabs", avgEstabs);
                record.put("annual_avg_emplvl", avgEmplvl);
                record.put("total_annual_wages", totalWages);
                record.put("annual_avg_wkly_wage", avgWklyWage);

                records.add(record);

              } catch (Exception e) {
                // Skip malformed records
                LOGGER.debug("Skipping malformed QCEW record: {}", e.getMessage());
              }

              // Log progress every 100K records
              if (recordCount % 100000 == 0) {
                LOGGER.info("Processed {} total records, {} county records extracted", recordCount, countyRecordCount);
              }
            }
          }
        }

        zis.closeEntry();
      }
    }

    LOGGER.info("Parsed {} county records from {} total QCEW records", countyRecordCount, recordCount);

    // Convert to Parquet and save
    if (!records.isEmpty()) {
      // fullParquetPath is already resolved, pass it directly
      storageProvider.writeAvroParquet(fullParquetPath, schema, records, "county_qcew");
      LOGGER.info("Converted QCEW data to Parquet: {} ({} records)", fullParquetPath, records.size());
    } else {
      LOGGER.warn("No county records found in QCEW data for year {}", year);
    }
  }

  /**
   * Parses a CSV line handling quoted fields.
   *
   * @param line CSV line to parse
   * @return Array of field values
   */
  private String[] parseCsvLine(String line) {
    List<String> fields = new ArrayList<>();
    StringBuilder currentField = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);

      if (c == '"') {
        inQuotes = !inQuotes;
      } else if (c == ',' && !inQuotes) {
        fields.add(currentField.toString());
        currentField.setLength(0);
      } else {
        currentField.append(c);
      }
    }

    // Add the last field
    fields.add(currentField.toString());

    return fields.toArray(new String[0]);
  }

  /**
   * Parses an integer from a string, returning null if empty or invalid.
   */
  private Integer parseIntOrNull(String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Parses a long from a string, returning null if empty or invalid.
   */
  private Long parseLongOrNull(String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
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
    requestBody.put("calculations", true); // Enable percent change calculations

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
    } else if (period.startsWith("Q")) {
      // Convert quarterly periods to first month of quarter
      // Q01 -> January (1), Q02 -> April (4), Q03 -> July (7), Q04 -> October (10)
      int quarter = Integer.parseInt(period.substring(1));
      return (quarter - 1) * 3 + 1;
    }
    return 1; // Default to January for annual or other data (avoid invalid month 0)
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
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
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
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
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
          // Set percent_change_year if available for earnings series
          record.put("percent_change_year", dataPoint.get("percent_change_year"));
        } else if (seriesId.equals(Series.EMPLOYMENT_COST_INDEX)) {
          record.put("employment_cost_index", dataPoint.get("value"));
          // Set percent_change_year if available for ECI series
          record.put("percent_change_year", dataPoint.get("percent_change_year"));
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
   *
   * <p>Note: This method only creates the parquet file. For the file to be discoverable
   * by FileSchema's table discovery, the EconRawToParquetConverter must be registered
   * with FileSchema (which happens in GovDataSchemaFactory.registerEconConverter()).
   *
   * <p>This method trusts FileSchema's conversion registry to prevent redundant conversions.
   * No defensive file existence check is needed here.
   */
  public void convertToParquet(String sourceDirPath, String targetFilePath) throws IOException {
    LOGGER.debug("Converting BLS data from {} to parquet: {}", sourceDirPath, targetFilePath);

    // Extract data type from filename
    String fileName = targetFilePath.substring(targetFilePath.lastIndexOf('/') + 1);

    // Read employment statistics JSON files and convert to employment_statistics.parquet
    if (fileName.equals("employment_statistics.parquet")) {
      convertEmploymentStatisticsToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("inflation_metrics.parquet")) {
      convertInflationMetricsToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("wage_growth.parquet")) {
      convertWageGrowthToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("regional_employment.parquet")) {
      convertRegionalEmploymentToParquet(sourceDirPath, targetFilePath);
    }
  }

  private void convertEmploymentStatisticsToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read employment statistics JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "employment_statistics.json");
    if (cacheStorageProvider.exists(jsonFilePath)) {
      try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
           java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(reader);

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

                // Extract percentage changes from BLS API calculations
                if (dataNode.has("calculations") && dataNode.get("calculations").has("pct_changes")) {
                  JsonNode pctChanges = dataNode.get("calculations").get("pct_changes");
                  if (pctChanges.has("1")) {
                    dataPoint.put("percent_change_month", pctChanges.get("1").asDouble());
                  }
                  if (pctChanges.has("12")) {
                    dataPoint.put("percent_change_year", pctChanges.get("12").asDouble());
                  }
                }

                dataPoints.add(dataPoint);
              }
            }
            seriesData.put(seriesId, dataPoints);
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Error reading BLS employment JSON file {}: {}", jsonFilePath, e.getMessage());
      }
    }

    // Write to parquet
    writeEmploymentStatisticsParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS employment data to parquet: {}", targetPath);
  }

  private void convertInflationMetricsToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read inflation metrics JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "inflation_metrics.json");
    if (cacheStorageProvider.exists(jsonFilePath)) {
      try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
           java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(reader);

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

                // Extract percentage changes from BLS API calculations
                if (dataNode.has("calculations") && dataNode.get("calculations").has("pct_changes")) {
                  JsonNode pctChanges = dataNode.get("calculations").get("pct_changes");
                  if (pctChanges.has("1")) {
                    dataPoint.put("percent_change_month", pctChanges.get("1").asDouble());
                  }
                  if (pctChanges.has("12")) {
                    dataPoint.put("percent_change_year", pctChanges.get("12").asDouble());
                  }
                }

                dataPoints.add(dataPoint);
              }
            }
            seriesData.put(seriesId, dataPoints);
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Error reading BLS inflation JSON file {}: {}", jsonFilePath, e.getMessage());
      }
    }

    // Write to parquet using inflation metrics schema
    writeInflationMetricsParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS inflation data to parquet: {}", targetPath);
  }

  private void convertWageGrowthToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read wage growth JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "wage_growth.json");
    if (cacheStorageProvider.exists(jsonFilePath)) {
      try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
           java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(reader);

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

                // Extract year-over-year percentage change from BLS API calculations
                // BLS API provides calculations.pct_changes.12 for 12-month change
                if (dataNode.has("calculations") && dataNode.get("calculations").has("pct_changes")) {
                  JsonNode pctChanges = dataNode.get("calculations").get("pct_changes");
                  if (pctChanges.has("12")) {
                    dataPoint.put("percent_change_year", pctChanges.get("12").asDouble());
                  }
                }

                dataPoints.add(dataPoint);
              }
            }
            seriesData.put(seriesId, dataPoints);
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Error reading BLS wage growth JSON file {}: {}", jsonFilePath, e.getMessage());
      }
    }

    // Write to parquet using wage growth schema
    writeWageGrowthParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS wage growth data to parquet: {}", targetPath);
  }

  private void convertRegionalEmploymentToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read regional employment JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "regional_employment.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No regional_employment.json found in {}", sourceDirPath);
      return;
    }

    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);

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

              // Extract percentage changes from BLS API calculations (if available)
              if (dataNode.has("calculations") && dataNode.get("calculations").has("pct_changes")) {
                JsonNode pctChanges = dataNode.get("calculations").get("pct_changes");
                if (pctChanges.has("1")) {
                  dataPoint.put("percent_change_month", pctChanges.get("1").asDouble());
                }
                if (pctChanges.has("12")) {
                  dataPoint.put("percent_change_year", pctChanges.get("12").asDouble());
                }
              }

              dataPoints.add(dataPoint);
            }
          }
          seriesData.put(seriesId, dataPoints);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error reading BLS regional employment JSON file {}: {}", jsonFilePath, e.getMessage(), e);
    }

    if (seriesData.isEmpty()) {
      LOGGER.warn("No data found in regional employment JSON file");
      return;
    }

    // Write to parquet using regional employment schema
    LOGGER.info("Converting regional employment data with {} series to parquet: {}", seriesData.size(), targetPath);
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
