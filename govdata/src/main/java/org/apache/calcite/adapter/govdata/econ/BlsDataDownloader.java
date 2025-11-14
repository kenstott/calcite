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
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;
import org.apache.calcite.adapter.govdata.CacheKey;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.util.Objects.requireNonNull;

/**
 * Downloads and converts BLS economic data to Parquet format.
 * Supports employment statistics, inflation metrics, wage growth, and regional employment data.
 */
public class BlsDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlsDataDownloader.class);
  private static final String BLS_API_BASE = "https://api.bls.gov/publicAPI/v2/";

  private final String apiKey;
  private final Set<String> enabledTables;

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

  // BLS area codes mapping: Publication Code → State(2) + BLS Area Code(5)
  // Format for SMU/ENU series: State Code (2 digits) + Area Code (5 digits)
  private static final Map<String, String> METRO_BLS_AREA_CODES = new HashMap<>();
  static {
    METRO_BLS_AREA_CODES.put("A100", "3693561");  // New York: State 36 + Area 93561
    METRO_BLS_AREA_CODES.put("A400", "0631080");  // Los Angeles: State 06 + Area 31080
    METRO_BLS_AREA_CODES.put("A207", "1716980");  // Chicago: State 17 + Area 16980
    METRO_BLS_AREA_CODES.put("A425", "4826420");  // Houston: State 48 + Area 26420
    METRO_BLS_AREA_CODES.put("A423", "0438060");  // Phoenix: State 04 + Area 38060
    METRO_BLS_AREA_CODES.put("A102", "4237980");  // Philadelphia: State 42 + Area 37980
    METRO_BLS_AREA_CODES.put("A426", "4841700");  // San Antonio: State 48 + Area 41700
    METRO_BLS_AREA_CODES.put("A421", "0641740");  // San Diego: State 06 + Area 41740
    METRO_BLS_AREA_CODES.put("A127", "4819100");  // Dallas: State 48 + Area 19100
    METRO_BLS_AREA_CODES.put("A429", "0641940");  // San Jose: State 06 + Area 41940
    METRO_BLS_AREA_CODES.put("A438", "4812420");  // Austin: State 48 + Area 12420
    METRO_BLS_AREA_CODES.put("A420", "1227260");  // Jacksonville: State 12 + Area 27260
    METRO_BLS_AREA_CODES.put("A103", "2514460");  // Boston: State 25 + Area 14460
    METRO_BLS_AREA_CODES.put("A428", "5342660");  // Seattle: State 53 + Area 42660
    METRO_BLS_AREA_CODES.put("A427", "0819740");  // Denver: State 08 + Area 19740
    METRO_BLS_AREA_CODES.put("A101", "1147900");  // Washington DC: State 11 + Area 47900
    METRO_BLS_AREA_CODES.put("A211", "2619820");  // Detroit: State 26 + Area 19820
    METRO_BLS_AREA_CODES.put("A104", "3917460");  // Cleveland: State 39 + Area 17460
    METRO_BLS_AREA_CODES.put("A212", "2733460");  // Minneapolis: State 27 + Area 33460
    METRO_BLS_AREA_CODES.put("A422", "1233100");  // Miami: State 12 + Area 33100
    METRO_BLS_AREA_CODES.put("A419", "1312060");  // Atlanta: State 13 + Area 12060
    METRO_BLS_AREA_CODES.put("A437", "4138900");  // Portland: State 41 + Area 38900
    METRO_BLS_AREA_CODES.put("A424", "0640140");  // Riverside: State 06 + Area 40140
    METRO_BLS_AREA_CODES.put("A320", "2941180");  // St. Louis: State 29 + Area 41180
    METRO_BLS_AREA_CODES.put("A319", "2412580");  // Baltimore: State 24 + Area 12580
    METRO_BLS_AREA_CODES.put("A433", "1245300");  // Tampa: State 12 + Area 45300
    METRO_BLS_AREA_CODES.put("A440", "0211260");  // Anchorage: State 02 + Area 11260
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
     * Format: CUUR{AREA_CODE}SA0
     * @param metroAreaCode Metro area publication code (e.g., "A100" for NYC)
     * @return CPI series ID or null if metro doesn't have CPI data
     */
    public static String getMetroCpiSeriesId(String metroAreaCode) {
      String cpiAreaCode = METRO_CPI_CODES.get(metroAreaCode);
      if (cpiAreaCode == null) {
        // Some metros don't have CPI data available
        return null;
      }
      return "CUUR" + cpiAreaCode + "SA0";
    }

    /**
     * Gets all metro area CPI series IDs for metros that have CPI data.
     */
    public static List<String> getAllMetroCpiSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      for (Map.Entry<String, String> entry : METRO_CPI_CODES.entrySet()) {
        if (entry.getValue() != null) {
          seriesIds.add(getMetroCpiSeriesId(entry.getKey()));
        }
      }
      return seriesIds;
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
      for (String stateFips : STATE_FIPS_MAP.values()) {
        for (String supersector : NAICS_SUPERSECTORS.keySet()) {
          seriesIds.add(getStateIndustryEmploymentSeriesId(stateFips, supersector));
        }
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
     * Format: SMU{STATE}{AREA}{SUPERSECTOR}{DATATYPE}
     * Example: SMU3693561000000000001 for NYC Total Nonfarm
     *
     * @param metroCode Metro area code (e.g., "A100" for NYC)
     * @param supersector NAICS supersector code (e.g., "00000000" for total nonfarm)
     * @return BLS metro industry employment series ID
     */
    public static String getMetroIndustryEmploymentSeriesId(String metroCode, String supersector) {
      String blsAreaCode = METRO_BLS_AREA_CODES.get(metroCode);
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
        for (String supersector : NAICS_SUPERSECTORS.keySet()) {
          seriesIds.add(getMetroIndustryEmploymentSeriesId(metroCode, supersector));
        }
      }
      return seriesIds;
    }

  }

  // Catalog-loaded geography and sector lists (replaces hardcoded maps)
  private final List<String> stateFipsList;
  private final List<String> regionCodesList;
  private final Map<String, MetroGeography> metroGeographiesMap;
  private final List<String> naicsSectorsList;

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
      tempStateFips = new ArrayList<>(STATE_FIPS_MAP.values());
      tempRegionCodes = new ArrayList<>(CENSUS_REGIONS.keySet());
      tempMetros = createMetroGeographiesFromHardcodedMaps();
      tempNaics = new ArrayList<>(NAICS_SUPERSECTORS.keySet());
    }

    this.stateFipsList = tempStateFips;
    this.regionCodesList = tempRegionCodes;
    this.metroGeographiesMap = tempMetros;
    this.naicsSectorsList = tempNaics;
  }

  @Override protected String getTableName() {
    return "employment_statistics";
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

  // Table name constants for filtering
  public static final String TABLE_EMPLOYMENT_STATISTICS = "employment_statistics";
  public static final String TABLE_INFLATION_METRICS = "inflation_metrics";
  public static final String TABLE_REGIONAL_CPI = "regional_cpi";
  public static final String TABLE_METRO_CPI = "metro_cpi";
  public static final String TABLE_STATE_INDUSTRY = "state_industry";
  public static final String TABLE_STATE_WAGES = "state_wages";
  public static final String TABLE_COUNTY_WAGES = "county_wages";
  public static final String TABLE_COUNTY_QCEW = "county_qcew";
  public static final String TABLE_METRO_INDUSTRY = "metro_industry";
  public static final String TABLE_METRO_WAGES = "metro_wages";
  public static final String TABLE_JOLTS_REGIONAL = "jolts_regional";
  public static final String TABLE_JOLTS_STATE = "jolts_state";
  public static final String TABLE_WAGE_GROWTH = "wage_growth";
  public static final String TABLE_REGIONAL_EMPLOYMENT = "regional_employment";

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
   * BLS API V2 limits for batching optimization.
   */
  private static final int MAX_SERIES_PER_REQUEST = 50;
  private static final int MAX_YEARS_PER_REQUEST = 20;

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
   * Fetches BLS data optimally batched by series (50 max) and years (20 max),
   * then splits results by individual year for caching.
   *
   * @param seriesIds List of BLS series IDs to fetch
   * @param uncachedYears Specific years that need fetching (maybe non-contiguous)
   * @return Map of year → JSON response for that year
   * @throws IOException If API request fails
   * @throws InterruptedException If interrupted while waiting
   */
  private Map<Integer, String> fetchAndSplitByYear(List<String> seriesIds, List<Integer> uncachedYears)
      throws IOException, InterruptedException {

    Map<Integer, String> resultsByYear = new HashMap<>();

    // Batch uncached years into contiguous ranges
    List<int[]> yearRanges = batchYearsIntoRanges(uncachedYears, MAX_YEARS_PER_REQUEST);

    LOGGER.info("Optimized fetch: {} series across {} years in {} batches",
                seriesIds.size(), uncachedYears.size(), yearRanges.size());

    // Batch by series (50 at a time)
    for (int seriesOffset = 0; seriesOffset < seriesIds.size(); seriesOffset += MAX_SERIES_PER_REQUEST) {
      int seriesEnd = Math.min(seriesOffset + MAX_SERIES_PER_REQUEST, seriesIds.size());
      List<String> seriesBatch = seriesIds.subList(seriesOffset, seriesEnd);

      // Fetch each year range
      for (int[] range : yearRanges) {
        int yearStart = range[0];
        int yearEnd = range[1];

        LOGGER.debug("Fetching series {}-{} for years {}-{}",
                    seriesOffset + 1, seriesEnd, yearStart, yearEnd);

        // Single API call for up to 50 series × up to 20 contiguous years
        String batchJson = fetchMultipleSeriesRaw(seriesBatch, yearStart, yearEnd);
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
   * @param seriesIds Full list of series IDs (can be >50)
   * @param uncachedYears List of years that need downloading
   * @return Map of year → combined JSON response
   */
  private Map<Integer, String> fetchAndSplitByYearLargeSeries(List<String> seriesIds, List<Integer> uncachedYears)
      throws IOException, InterruptedException {

    Map<Integer, String> resultsByYear = new HashMap<>();
    List<int[]> yearRanges = batchYearsIntoRanges(uncachedYears, MAX_YEARS_PER_REQUEST);

    LOGGER.info("Optimized fetch: {} series across {} years in {} year-batches, {} series-batches",
                seriesIds.size(), uncachedYears.size(), yearRanges.size(),
                (seriesIds.size() + MAX_SERIES_PER_REQUEST - 1) / MAX_SERIES_PER_REQUEST);

    // Batch series into groups of 50
    for (int seriesOffset = 0; seriesOffset < seriesIds.size(); seriesOffset += MAX_SERIES_PER_REQUEST) {
      int seriesEnd = Math.min(seriesOffset + MAX_SERIES_PER_REQUEST, seriesIds.size());
      List<String> seriesBatch = seriesIds.subList(seriesOffset, seriesEnd);

      LOGGER.info("Processing series batch {}/{} ({} series)",
                  (seriesOffset / MAX_SERIES_PER_REQUEST) + 1,
                  (seriesIds.size() + MAX_SERIES_PER_REQUEST - 1) / MAX_SERIES_PER_REQUEST,
                  seriesBatch.size());

      // Fetch each year range for this series batch
      for (int[] range : yearRanges) {
        int yearStart = range[0];
        int yearEnd = range[1];

        String batchJson = fetchMultipleSeriesRaw(seriesBatch, yearStart, yearEnd);
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
  private void downloadAllTables(int startYear, int endYear, Set<String> enabledTables) throws IOException, InterruptedException {
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

    // Download county wages
    if (enabledTables == null || enabledTables.contains(TABLE_COUNTY_WAGES)) {
      downloadCountyWages(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out - saves ~6,000 counties!)", TABLE_COUNTY_WAGES);
    }

    // Download county QCEW (comprehensive county-level employment and wage data)
    if (enabledTables == null || enabledTables.contains(TABLE_COUNTY_QCEW)) {
      downloadCountyQcew(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_COUNTY_QCEW);
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

    // Download JOLTS state data
    if (enabledTables == null || enabledTables.contains(TABLE_JOLTS_STATE)) {
      downloadJoltsState(startYear, endYear);
    } else {
      LOGGER.info("Skipping {} (filtered out)", TABLE_JOLTS_STATE);
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

    // Download reference tables (always downloaded, not subject to filtering)
    LOGGER.info("Downloading JOLTS reference tables (industries, data elements)");
    downloadJoltsIndustries();
    downloadJoltsDataelements();
    LOGGER.info("BLS data download completed");
  }

  /**
   * Converts all downloaded BLS data to Parquet format for the specified year range.
   * Uses the enabledTables set passed to the constructor to filter which tables to convert.
   * Uses IterationDimension pattern for declarative multi-dimensional iteration.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   */
  @Override public void convertAll(int startYear, int endYear) {
    LOGGER.info("Converting BLS data for years {}-{}", startYear, endYear);

    // Define all BLS tables (12 tables)
    List<String> tablesToConvert =
        Arrays.asList(TABLE_EMPLOYMENT_STATISTICS,
        TABLE_INFLATION_METRICS,
        TABLE_REGIONAL_CPI,
        TABLE_METRO_CPI,
        TABLE_STATE_INDUSTRY,
        TABLE_STATE_WAGES,
        TABLE_METRO_INDUSTRY,
        TABLE_METRO_WAGES,
        TABLE_JOLTS_REGIONAL,
        TABLE_COUNTY_WAGES,
        TABLE_JOLTS_STATE,
        TABLE_WAGE_GROWTH);

    // Convert each enabled table using IterationDimension pattern
    for (String tableName : tablesToConvert) {
      if (enabledTables == null || enabledTables.contains(tableName)) {
        // metro_wages uses direct CSV→Parquet conversion (no intermediate JSON)
        if (TABLE_METRO_WAGES.equals(tableName)) {
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
            (cacheKey, vars, jsonPath, parquetPath) -> {
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
  public void downloadEmploymentStatistics(int startYear, int endYear) throws IOException, InterruptedException {

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
            case "frequency": return java.util.Arrays.asList("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch for this year
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, List.of(year));
          String rawJson = resultsByYear.get(year);

          if (rawJson != null) {
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
  public void downloadRegionalCpi(int startYear, int endYear) throws IOException, InterruptedException {
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
          switch (dimensionName) {
            case "year":
              return yearRange(startYear, endYear);
            default:
              return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch all regions for this year
          List<Integer> singleYearList = java.util.Collections.singletonList(year);
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, singleYearList);

          String rawJson = resultsByYear.get(year);
          if (rawJson != null) {
            // Save to cache
            cacheStorageProvider.writeFile(jsonPath, rawJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
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
  public void downloadMetroCpi(int startYear, int endYear) throws IOException, InterruptedException {
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
          switch (dimensionName) {
            case "year":
              return yearRange(startYear, endYear);
            default:
              return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch all metros for this year
          List<Integer> singleYearList = java.util.Collections.singletonList(year);
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, singleYearList);

          String rawJson = resultsByYear.get(year);
          if (rawJson != null) {
            // Save to cache
            cacheStorageProvider.writeFile(jsonPath, rawJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
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
  public void downloadStateIndustryEmployment(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading state industry employment for {} states × {} sectors ({} series) for {}-{}",
                STATE_FIPS_MAP.size(), NAICS_SUPERSECTORS.size(),
                STATE_FIPS_MAP.size() * NAICS_SUPERSECTORS.size(), startYear, endYear);

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
            case "frequency": return java.util.Arrays.asList("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch for this year (with large series batching)
          Map<Integer, String> resultsByYear = fetchAndSplitByYearLargeSeries(seriesIds, List.of(year));
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
  public void downloadStateWages(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading state wages from QCEW CSV files for {}-{}", startYear, endYear);

    String tableName = "state_wages";

    // QCEW data only available from 1990 forward
    int effectiveStartYear = Math.max(startYear, 1990);

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(effectiveStartYear, endYear);
            case "frequency": return java.util.Arrays.asList("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Get QCEW ZIP download metadata from schema
          Map<String, Object> metadata = loadTableMetadata(tableName);
          com.fasterxml.jackson.databind.JsonNode downloadNode =
              (com.fasterxml.jackson.databind.JsonNode) metadata.get("download");
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
  public void downloadCountyWages(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading county wages from QCEW CSV files for {}-{}", startYear, endYear);

    String tableName = "county_wages";

    // Get QCEW ZIP download metadata from state_wages table (shared download)
    Map<String, Object> stateWagesMetadata = loadTableMetadata("state_wages");
    com.fasterxml.jackson.databind.JsonNode downloadNode =
        (com.fasterxml.jackson.databind.JsonNode) stateWagesMetadata.get("download");

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "frequency": return java.util.Arrays.asList("quarterly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
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
   * @throws IOException if download or parsing fails
   */
  public void downloadCountyQcew(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading county QCEW data from BLS CSV files for {}-{}", startYear, endYear);

    String tableName = "county_qcew";

    // Get QCEW ZIP download metadata from state_wages table (shared download)
    Map<String, Object> stateWagesMetadata = loadTableMetadata("state_wages");
    com.fasterxml.jackson.databind.JsonNode downloadNode =
        (com.fasterxml.jackson.databind.JsonNode) stateWagesMetadata.get("download");

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "frequency": return java.util.Arrays.asList("quarterly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
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
  public void downloadMetroIndustryEmployment(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading metro industry employment for {} metros × {} sectors ({} series) for {}-{}",
                METRO_AREA_CODES.size(), NAICS_SUPERSECTORS.size(),
                METRO_AREA_CODES.size() * NAICS_SUPERSECTORS.size(), startYear, endYear);

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
            case "frequency": return java.util.Arrays.asList("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch for this year (with large series batching)
          Map<Integer, String> resultsByYear = fetchAndSplitByYearLargeSeries(seriesIds, List.of(year));
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

    String tableName = TABLE_METRO_WAGES;

    // QCEW data only available from 1990 forward
    int effectiveStartYear = Math.max(startYear, 1990);

    // Process both annual and quarterly data
    String[] frequencies = {"annual", "qtrly"};

    for (int year = effectiveStartYear; year <= endYear; year++) {
      for (String frequency : frequencies) {
        try {
          // Build variables map
          Map<String, String> variables = new HashMap<>();
          variables.put("year", String.valueOf(year));
          variables.put("frequency", frequency);

          // Check if already converted using cache manifest
          Map<String, Object> metadata = loadTableMetadata(tableName);
          String pattern = (String) metadata.get("pattern");
          String parquetPath =
              storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, variables));

          CacheKey cacheKey = new CacheKey(tableName, variables);
          if (isParquetConvertedOrExists(cacheKey, null, parquetPath)) {
            LOGGER.info("Skipping {} for year {} {} - already converted", tableName, year, frequency);
            continue;
          }

          // Use new CSV→Parquet conversion method
          convertCsvToParquet(tableName, variables);

          // Mark as converted in cache manifest
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
          cacheManifest.save(operatingDirectory);

          LOGGER.info("Successfully converted {} for year {} {} to Parquet", tableName, year, frequency);

        } catch (IOException e) {
          LOGGER.error("Failed to convert {} for year {} {}: {}",
              tableName, year, frequency, e.getMessage(), e);
          // Continue with next year/frequency instead of failing completely
        }
      }
    }

    LOGGER.info("Metro wages conversion complete for years {}-{}", effectiveStartYear, endYear);
  }

  /**
   * Downloads QCEW bulk CSV file from BLS for a given year and frequency.
   *
   * <p>Downloads bulk QCEW files containing all metro area wage data in a single ZIP file.
   * This replaces per-metro API calls which return HTTP 404 errors for metro C-codes.
   *
   * <p>Bulk files are cached in: a source=econ/type=qcew_bulk/{year}/{frequency}_singlefile.zip
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

    // Build cache path: source=econ/type=qcew_bulk/{year}/{frequency}_singlefile.zip
    String relativePath = String.format("type=qcew_bulk/year=%d/%s_singlefile.zip", year, frequency);
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);

    // Check if a file already cached
    if (cacheStorageProvider.exists(fullPath)) {
      LOGGER.info("Using cached QCEW bulk {} file for {}: {}", frequency, year, relativePath);
      return fullPath;
    }

    // Download bulk file from BLS
    String url =
                                String.format("https://data.bls.gov/cew/data/files/%d/csv/%d_%s_singlefile.zip", year, year, frequency);

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
   */
  public void downloadJoltsRegional(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading JOLTS regional data from BLS FTP flat files for {}-{}", startYear, endYear);

    String tableName = "jolts_regional";

    // JOLTS data only available from 2001 forward
    int effectiveStartYear = Math.max(startYear, 2001);

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(effectiveStartYear, endYear);
            case "frequency": return java.util.Arrays.asList("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          String joltsFtpPath = "type=jolts_ftp/jolts_series.txt";
          downloadJoltsFtpFileIfNeeded(joltsFtpPath, "https://download.bls.gov/pub/time.series/jt/jt.series");

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
   */
  public void downloadJoltsState(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading JOLTS state data from BLS FTP flat files for {}-{}", startYear, endYear);

    String tableName = "jolts_state";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "frequency": return java.util.Arrays.asList("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
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
   * Downloads JOLTS industry code reference table from BLS FTP.
   * Downloads once (not partitioned by year).
   *
   * @throws IOException if download fails
   */
  public void downloadJoltsIndustries() throws IOException {
    LOGGER.info("Downloading JOLTS industry reference data from BLS FTP");

    String outputDirPath = "type=reference";
    String jsonFilePath = outputDirPath + "/jolts_industries.json";
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("year", String.valueOf(-1));

    CacheKey cacheKey = new CacheKey("reference_jolts_industries", cacheParams);

    // Check if already cached (use year=-1 for non-partitioned reference data)
    if (isCachedOrExists(cacheKey)) {
      LOGGER.info("JOLTS industry reference data already cached");
      return;
    }

    String url = "https://download.bls.gov/pub/time.series/jt/jt.industry";
    String ftpPath = "type=jolts_ftp/jt.industry";

    // Download file (will be cached by downloadJoltsFtpFileIfNeeded)
    byte[] data = downloadJoltsFtpFileIfNeeded(ftpPath, url);

    // Parse tab-delimited file
    List<Map<String, Object>> industries = getMaps(data, "industry_code", "industry_name");

    LOGGER.info("Parsed {} JOLTS industries from reference file", industries.size());

    // Convert to JSON
    String json;
    try {
      json = MAPPER.writeValueAsString(industries);
    } catch (Exception e) {
      LOGGER.error("Failed to serialize JOLTS industries to JSON: {}", e.getMessage());
      return;
    }

    // Save to cache (use year=-1 for non-partitioned reference data)
    saveToCache("reference_jolts_industries", -1, cacheParams, jsonFilePath, json);
  }

  private static List<Map<String, Object>> getMaps(byte[] data, String industry_code, String industry_name) throws IOException {
    List<Map<String, Object>> industries = new ArrayList<>();
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

        Map<String, Object> industry = new HashMap<>();
        industry.put(industry_code, fields[0].trim());
        industry.put(industry_name, fields[1].trim());
        industries.add(industry);
      }
    }
    return industries;
  }

  /**
   * Downloads JOLTS data element code reference table from BLS FTP.
   * Downloads once (not partitioned by year).
   *
   * @throws IOException if download fails
   */
  public void downloadJoltsDataelements() throws IOException {
    LOGGER.info("Downloading JOLTS data element reference data from BLS FTP");

    String outputDirPath = "type=reference";
    String jsonFilePath = outputDirPath + "/jolts_dataelements.json";
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("year", String.valueOf(-1));

    CacheKey cacheKey = new CacheKey("reference_jolts_dataelements", cacheParams);

    // Check if already cached (use year=-1 for non-partitioned reference data)
    if (isCachedOrExists(cacheKey)) {
      LOGGER.info("JOLTS data element reference data already cached");
      return;
    }

    String url = "https://download.bls.gov/pub/time.series/jt/jt.dataelement";
    String ftpPath = "type=jolts_ftp/jt.dataelement";

    // Download file (will be cached by downloadJoltsFtpFileIfNeeded)
    byte[] data = downloadJoltsFtpFileIfNeeded(ftpPath, url);

    // Parse tab-delimited file
    List<Map<String, Object>> dataElements = getMaps(data, "dataelement_code", "dataelement_text");

    LOGGER.info("Parsed {} JOLTS data elements from reference file", dataElements.size());

    // Convert to JSON
    String json;
    try {
      json = MAPPER.writeValueAsString(dataElements);
    } catch (Exception e) {
      LOGGER.error("Failed to serialize JOLTS data elements to JSON: {}", e.getMessage());
      return;
    }

    // Save to cache (use year=-1 for non-partitioned reference data)
    saveToCache("reference_jolts_dataelements", -1, cacheParams, jsonFilePath, json);
  }

  /**
   * Downloads inflation metrics data and converts to Parquet.
   */
  public void downloadInflationMetrics(int startYear, int endYear) throws IOException, InterruptedException {

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
            case "frequency": return java.util.Arrays.asList("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch for this year
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, List.of(year));
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
  public void downloadWageGrowth(int startYear, int endYear) throws IOException, InterruptedException {

    final List<String> seriesIds =
        List.of(Series.AVG_HOURLY_EARNINGS,
        Series.EMPLOYMENT_COST_INDEX);

    String tableName = "wage_growth";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "frequency": return java.util.Arrays.asList("monthly");
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Batch fetch for this year
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, List.of(year));
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
  public void downloadRegionalEmployment(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading regional employment data for all 51 states/jurisdictions (years {}-{})", startYear, endYear);

    String tableName = "regional_employment";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          switch (dimensionName) {
            case "year": return yearRange(startYear, endYear);
            case "state_fips": return new ArrayList<>(STATE_FIPS_MAP.values());
            default: return null;
          }
        },
        (cacheKey, vars, jsonPath, parquetPath) -> {
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
          Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, List.of(year));
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
   * @param fullZipPath Full path to cached ZIP file containing CSV
   * @param fullParquetPath Full path for the output Parquet file (already resolved)
   * @param year Year of the data
   * @throws IOException if parsing or conversion fails
   */
  private void parseAndConvertQcewToParquet(String fullZipPath, String fullParquetPath, int year) throws IOException {
    LOGGER.info("Converting QCEW CSV to Parquet via DuckDB for year {}", year);

    // Extract CSV from ZIP to temp file (DuckDB cannot read CSV from ZIP in S3)
    String csvTempPath = extractCsvFromZip(fullZipPath, year);

    try {
      // Build DuckDB SQL to read extracted CSV, filter, and write to Parquet
      String sql =
          String.format("COPY (\n"
    +
          "  SELECT\n"
    +
          "    q.area_fips,\n"
    +
          "    q.own_code,\n"
    +
          "    q.industry_code,\n"
    +
          "    q.agglvl_code,\n"
    +
          "    TRY_CAST(q.annual_avg_estabs AS INTEGER) AS annual_avg_estabs,\n"
    +
          "    TRY_CAST(q.annual_avg_emplvl AS INTEGER) AS annual_avg_emplvl,\n"
    +
          "    TRY_CAST(q.total_annual_wages AS BIGINT) AS total_annual_wages,\n"
    +
          "    TRY_CAST(q.annual_avg_wkly_wage AS INTEGER) AS annual_avg_wkly_wage\n"
    +
          "  FROM read_csv_auto('%s') q\n"
    +
          "  WHERE CAST(q.agglvl_code AS VARCHAR) LIKE '7%%'\n"
    +  // County-level aggregations (70-78)
          "    AND length(q.area_fips) = 5\n"
    +     // 5-digit FIPS codes only
          "    AND q.area_fips != 'US000'\n"
    +      // Exclude national aggregate
          ") TO '%s' (FORMAT PARQUET);",
          csvTempPath.replace("'", "''"),  // Escape single quotes in path
          fullParquetPath.replace("'", "''"));

      // Execute via DuckDB
      executeDuckDBSql(sql, "QCEW county CSV to Parquet conversion");

      LOGGER.info("Successfully converted QCEW data to Parquet: {}", fullParquetPath);
    } finally {
      // Clean up temp CSV file
      Files.deleteIfExists(Paths.get(csvTempPath));
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
    ((CacheManifest) cacheManifest).markCached(qcewCacheKey, qcewZipPath, zipData.length, refreshAfter, "immutable_historical");
    cacheManifest.save(operatingDirectory);

    LOGGER.info("Downloaded and cached QCEW CSV for year {} ({} MB)", year, zipData.length / (1024 * 1024));

  }

  /**
   * Parses QCEW CSV and extracts state-level wage data using DuckDB.
   * Filters for agglvl_code = 50 (state level), own_code = 0 (all ownership),
   * industry_code = 10 (total all industries).
   *
   * @param fullZipPath Full path to cached ZIP file containing CSV
   * @param fullParquetPath Full path for the output Parquet file
   * @param year Year of data
   * @throws IOException if conversion fails
   */
  private void parseQcewForStateWages(String fullZipPath, String fullParquetPath, int year) throws IOException {
    LOGGER.info("Converting QCEW CSV to Parquet via DuckDB for state wages year {}", year);

    // Extract CSV from ZIP to temp file (DuckDB cannot read CSV from ZIP in S3)
    String csvTempPath = extractCsvFromZip(fullZipPath, year);

    try {
      // Get the resource path for state_fips.json
      String stateFipsJsonPath = requireNonNull(getClass().getResource("/state_fips.json")).getPath();

      // Load SQL from resource and substitute parameters
      String sql =
          substituteSqlParameters(loadSqlResource("/sql/bls/convert_state_wages.sql"),
          ImmutableMap.of(
              "year", String.valueOf(year),
              "csvPath", csvTempPath.replace("'", "''"),
              "stateFipsPath", stateFipsJsonPath.replace("'", "''"),
              "parquetPath", fullParquetPath.replace("'", "''")));

      // Execute via DuckDB
      executeDuckDBSql(sql, "QCEW state wages CSV to Parquet conversion");

      LOGGER.info("Successfully converted state wages to Parquet: {}", fullParquetPath);
    } finally {
      // Clean up temp CSV file
      Files.deleteIfExists(Paths.get(csvTempPath));
    }
  }

  /**
   * Parses QCEW CSV and extracts county-level wage data using DuckDB.
   * Filters for agglvl_code = 70 (county level), own_code = 0, industry_code = 10.
   * Most granular wage data available (~6,038 counties).
   *
   * @param fullZipPath Full path to cached ZIP file containing CSV
   * @param fullParquetPath Full path for the output Parquet file
   * @param year Year of data
   * @throws IOException if conversion fails
   */
  private void parseQcewForCountyWages(String fullZipPath, String fullParquetPath, int year) throws IOException {
    LOGGER.info("Converting QCEW CSV to Parquet via DuckDB for county wages year {}", year);

    // Extract CSV from ZIP to temp file (DuckDB cannot read CSV from ZIP in S3)
    String csvTempPath = extractCsvFromZip(fullZipPath, year);

    try {
      // Get the resource path for state_fips.json
      String stateFipsJsonPath = requireNonNull(getClass().getResource("/state_fips.json")).getPath();

      // Load SQL from resource and substitute parameters
      String sql =
          substituteSqlParameters(loadSqlResource("/sql/bls/convert_county_wages.sql"),
          ImmutableMap.of(
              "year", String.valueOf(year),
              "csvPath", csvTempPath.replace("'", "''"),
              "stateFipsPath", stateFipsJsonPath.replace("'", "''"),
              "parquetPath", fullParquetPath.replace("'", "''")));

      // Execute via DuckDB
      executeDuckDBSql(sql, "QCEW county wages CSV to Parquet conversion");

      LOGGER.info("Successfully converted county wages to Parquet: {}", fullParquetPath);
    } finally {
      // Clean up temp CSV file
      Files.deleteIfExists(Paths.get(csvTempPath));
    }
  }

  /**
   * Extracts CSV file from ZIP archive to a temporary file.
   * DuckDB cannot read CSV files directly from ZIP archives in S3,
   * so we need to extract first.
   *
   * @param fullZipPath Full path to ZIP file in cache storage
   * @param year Year for CSV filename pattern
   * @return Path to extracted temporary CSV file
   * @throws IOException if extraction fails
   */
  private String extractCsvFromZip(String fullZipPath, int year) throws IOException {
    LOGGER.info("Extracting CSV from ZIP: {}", fullZipPath);

    // Create temp file for CSV
    Path tempCsv = Files.createTempFile("qcew_" + year + "_", ".csv");
    String tempCsvPath = tempCsv.toString();

    try (InputStream zipInputStream = cacheStorageProvider.openInputStream(fullZipPath);
         ZipInputStream zis = new ZipInputStream(zipInputStream)) {

      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String entryName = entry.getName();

        // Look for the CSV file in the ZIP (format: YYYY.annual.singlefile.csv)
        if (entryName.endsWith(".csv") && entryName.contains(String.valueOf(year))) {
          LOGGER.info("Found CSV in ZIP: {}", entryName);

          // Copy to temp file
          try (FileOutputStream fos = new FileOutputStream(tempCsv.toFile())) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = zis.read(buffer)) > 0) {
              fos.write(buffer, 0, len);
            }
          }

          LOGGER.info("Extracted CSV to temp file: {}", tempCsvPath);
          return tempCsvPath;
        }

        zis.closeEntry();
      }

      throw new IOException("Could not find CSV file for year " + year + " in ZIP: " + fullZipPath);

    } catch (IOException e) {
      // Clean up temp file if extraction failed
      Files.deleteIfExists(tempCsv);
      throw e;
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
    ((CacheManifest) cacheManifest).markCached(joltsFtpCacheKey, ftpPath, data.length, refreshAfter, "monthly_refresh");
    cacheManifest.save(operatingDirectory);

    LOGGER.info("Downloaded and cached JOLTS FTP file ({} KB)", data.length / 1024);

    return data;
  }

  /**
   * Parses JOLTS FTP flat files and extracts regional data for a given year.
   * Downloads and parses tab-delimited data files for job openings, hires, separations, quits, layoffs.
   * Filters for 4 Census regions (NE, MW, SO, WE) and converts to JSON format.
   */
  private String parseJoltsFtpForRegional(int year) throws IOException {
    // Regional series patterns (state codes in positions 10-11 of series ID)
    String[] regionCodes = {"NE", "MW", "SO", "WE"};
    String[] regionNames = {"Northeast", "Midwest", "South", "West"};

    // Data element files to download
    String[] dataFiles = {
        "jt.data.2.JobOpenings",
        "jt.data.3.Hires",
        "jt.data.4.TotalSeparations",
        "jt.data.5.Quits",
        "jt.data.6.LayoffsDischarges"
    };

    Map<String, Map<String, Object>> regionalDataMap = new HashMap<>();

    for (String dataFile : dataFiles) {
      String ftpPath = "type=jolts_ftp/" + dataFile;
      String url = "https://download.bls.gov/pub/time.series/jt/" + dataFile;

      byte[] data = downloadJoltsFtpFileIfNeeded(ftpPath, url);

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
      LOGGER.warn("No regional JOLTS data found for year {} (processed {} data files)", year, dataFiles.length);
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
   * Downloads and parses tab-delimited data files for job openings, hires, separations, quits, layoffs.
   * Filters for 51 states (including DC) using state codes 01-56 and converts to JSON format.
   */
  private String parseJoltsFtpForState(int year) throws IOException {
    // Data element files to download
    String[] dataFiles = {
        "jt.data.2.JobOpenings",
        "jt.data.3.Hires",
        "jt.data.4.TotalSeparations",
        "jt.data.5.Quits",
        "jt.data.6.LayoffsDischarges"
    };

    // Map from state_code to state data
    Map<String, Map<String, Object>> stateDataMap = new HashMap<>();

    for (String dataFile : dataFiles) {
      String ftpPath = "type=jolts_ftp/" + dataFile;
      String url = "https://download.bls.gov/pub/time.series/jt/" + dataFile;

      byte[] data = downloadJoltsFtpFileIfNeeded(ftpPath, url);

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
      LOGGER.warn("No state JOLTS data found for year {} (processed {} data files)", year, dataFiles.length);
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
   * Fetches raw JSON response for multiple BLS series in a single API call.
   * Uses the generic retry mechanism from AbstractGovDataDownloader.
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
   */
  private List<String> loadStateFipsFromCatalog() throws IOException {
    List<String> stateFips = new ArrayList<>();
    String pattern = "type=reference/geo_type=state/bls_geographies.parquet";
    String fullPath = storageProvider.resolvePath(parquetDirectory, pattern);

    try (java.sql.Connection duckdb = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
      String query = String.format(
          "SELECT state_fips FROM read_parquet('%s') ORDER BY state_fips", fullPath);

      try (java.sql.Statement stmt = duckdb.createStatement();
           java.sql.ResultSet rs = stmt.executeQuery(query)) {
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
   */
  private List<String> loadRegionCodesFromCatalog() throws IOException {
    List<String> regionCodes = new ArrayList<>();
    String pattern = "type=reference/geo_type=region/bls_geographies.parquet";
    String fullPath = storageProvider.resolvePath(parquetDirectory, pattern);

    try (java.sql.Connection duckdb = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
      String query = String.format(
          "SELECT region_code FROM read_parquet('%s') ORDER BY region_code", fullPath);

      try (java.sql.Statement stmt = duckdb.createStatement();
           java.sql.ResultSet rs = stmt.executeQuery(query)) {
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
   */
  private Map<String, MetroGeography> loadMetroGeographiesFromCatalog() throws IOException {
    Map<String, MetroGeography> metros = new HashMap<>();
    String pattern = "type=reference/geo_type=metro/bls_geographies.parquet";
    String fullPath = storageProvider.resolvePath(parquetDirectory, pattern);

    try (java.sql.Connection duckdb = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
      String query = String.format(
          "SELECT metro_publication_code, geo_name, metro_cpi_area_code, metro_bls_area_code "
          + "FROM read_parquet('%s') ORDER BY metro_publication_code", fullPath);

      try (java.sql.Statement stmt = duckdb.createStatement();
           java.sql.ResultSet rs = stmt.executeQuery(query)) {
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
   */
  private List<String> loadNaicsSectorsFromCatalog() throws IOException {
    List<String> sectors = new ArrayList<>();
    String pattern = "type=reference/bls_naics_sectors.parquet";
    String fullPath = storageProvider.resolvePath(parquetDirectory, pattern);

    try (java.sql.Connection duckdb = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
      String query = String.format(
          "SELECT supersector_code FROM read_parquet('%s') ORDER BY supersector_code", fullPath);

      try (java.sql.Statement stmt = duckdb.createStatement();
           java.sql.ResultSet rs = stmt.executeQuery(query)) {
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
      String blsCode = METRO_BLS_AREA_CODES.get(metroCode);
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
      return java.util.Arrays.asList("monthly");
    }
    return super.getVariableValues(tableName, varName);
  }

  // ===== Metadata-Driven Employment Statistics Methods =====

  /**
   * Downloads BLS reference tables (JOLTS industries and dataelements).
   * Uses year=-1 sentinel value for reference tables without year dimension.
   */
  @Override public void downloadReferenceData() throws IOException {
    LOGGER.info("Downloading BLS JOLTS reference tables");

    // Download JOLTS industries from BLS FTP
    downloadJoltsIndustries();

    // Download and convert reference_jolts_industries
    String joltsIndustriesParquetPath =
        storageProvider.resolvePath(parquetDirectory, "type=reference/jolts_industries.parquet");
    String joltsIndustriesRawPath =
        cacheStorageProvider.resolvePath(cacheDirectory, "type=reference/jolts_industries.json");

    Map<String, String> joltsIndustriesParams = new HashMap<>();
    joltsIndustriesParams.put("year", String.valueOf(-1));
    CacheKey joltsIndustriesCacheKey = new CacheKey("reference_jolts_industries", joltsIndustriesParams);

    if (!isParquetConvertedOrExists(joltsIndustriesCacheKey,
        joltsIndustriesRawPath, joltsIndustriesParquetPath)) {
      Map<String, String> variables = new HashMap<>();
      convertCachedJsonToParquet("reference_jolts_industries", variables);
      cacheManifest.markParquetConverted(joltsIndustriesCacheKey,
          joltsIndustriesParquetPath);
      LOGGER.info("Converted reference_jolts_industries to parquet");
    } else {
      LOGGER.info("reference_jolts_industries already converted, skipping");
    }

    // Download JOLTS data elements from BLS FTP
    downloadJoltsDataelements();

    // Download and convert reference_jolts_dataelements
    String joltsDataelementsParquetPath =
        storageProvider.resolvePath(parquetDirectory, "type=reference/jolts_dataelements.parquet");
    String joltsDataelementsRawPath =
        cacheStorageProvider.resolvePath(cacheDirectory, "type=reference/jolts_dataelements.json");

    Map<String, String> joltsDataelementsParams = new HashMap<>();
    joltsDataelementsParams.put("year", String.valueOf(-1));
    CacheKey joltsDataelementsCacheKey = new CacheKey("reference_jolts_dataelements", joltsDataelementsParams);

    if (!isParquetConvertedOrExists(joltsDataelementsCacheKey,
        joltsDataelementsRawPath, joltsDataelementsParquetPath)) {
      Map<String, String> variables = new HashMap<>();
      convertCachedJsonToParquet("reference_jolts_dataelements", variables);
      cacheManifest.markParquetConverted(joltsDataelementsCacheKey,
          joltsDataelementsParquetPath);
      LOGGER.info("Converted reference_jolts_dataelements to parquet");
    } else {
      LOGGER.info("reference_jolts_dataelements already converted, skipping");
    }

    // Generate BLS geographies reference table from hardcoded maps
    generateBlsGeographiesReference();

    // Generate BLS NAICS sectors reference table from hardcoded map
    generateBlsNaicsSectorsReference();

    LOGGER.info("Completed BLS reference tables download");
  }

  /**
   * Generates BLS geographies reference table from hardcoded maps.
   * Consolidates STATE_FIPS_MAP, CENSUS_REGIONS, METRO_AREA_CODES, METRO_CPI_CODES, and METRO_BLS_AREA_CODES
   * into catalog-driven parquet files partitioned by geo_type.
   */
  private void generateBlsGeographiesReference() throws IOException {
    LOGGER.info("Generating BLS geographies reference table");

    try (java.sql.Connection duckdb = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
      // Create in-memory table with all geography data
      String createTableSql = "CREATE TABLE bls_geographies ("
          + "geo_code VARCHAR, "
          + "geo_name VARCHAR, "
          + "geo_type VARCHAR, "
          + "state_fips VARCHAR, "
          + "region_code VARCHAR, "
          + "metro_publication_code VARCHAR, "
          + "metro_cpi_area_code VARCHAR, "
          + "metro_bls_area_code VARCHAR)";

      try (java.sql.Statement stmt = duckdb.createStatement()) {
        stmt.execute(createTableSql);

        // Insert state data
        for (java.util.Map.Entry<String, String> entry : STATE_FIPS_MAP.entrySet()) {
          String insertSql = String.format(
              "INSERT INTO bls_geographies VALUES ('%s', '%s', 'state', '%s', NULL, NULL, NULL, NULL)",
              entry.getKey(), entry.getKey(), entry.getValue());
          stmt.execute(insertSql);
        }

        // Insert census region data
        for (java.util.Map.Entry<String, String> entry : CENSUS_REGIONS.entrySet()) {
          String insertSql = String.format(
              "INSERT INTO bls_geographies VALUES ('%s', '%s', 'region', NULL, '%s', NULL, NULL, NULL)",
              entry.getKey(), entry.getValue(), entry.getKey());
          stmt.execute(insertSql);
        }

        // Insert metro area data (combine all metro maps)
        for (java.util.Map.Entry<String, String> entry : METRO_AREA_CODES.entrySet()) {
          String metroCode = entry.getKey();
          String metroName = entry.getValue();
          String cpiCode = METRO_CPI_CODES.get(metroCode);
          String blsAreaCode = METRO_BLS_AREA_CODES.get(metroCode);

          String insertSql = String.format(
              "INSERT INTO bls_geographies VALUES ('%s', '%s', 'metro', NULL, NULL, '%s', %s, %s)",
              metroCode,
              metroName.replace("'", "''"),  // Escape single quotes
              metroCode,
              cpiCode != null ? "'" + cpiCode + "'" : "NULL",
              blsAreaCode != null ? "'" + blsAreaCode + "'" : "NULL");
          stmt.execute(insertSql);
        }

        // Write partitioned parquet files by geo_type
        for (String geoType : java.util.Arrays.asList("state", "region", "metro")) {
          String parquetPath = storageProvider.resolvePath(parquetDirectory,
              "type=reference/geo_type=" + geoType + "/bls_geographies.parquet");

          // Ensure parent directory exists
          ensureParentDirectory(parquetPath);

          String copySql = String.format(
              "COPY (SELECT * FROM bls_geographies WHERE geo_type = '%s' ORDER BY geo_code) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)",
              geoType, parquetPath);

          stmt.execute(copySql);

          long count = 0;
          try (java.sql.ResultSet rs = stmt.executeQuery(
              String.format("SELECT count(*) FROM bls_geographies WHERE geo_type = '%s'", geoType))) {
            if (rs.next()) {
              count = rs.getLong(1);
            }
          }

          LOGGER.info("Generated {} BLS {} geographies to {}", count, geoType, parquetPath);
        }
      }
    } catch (java.sql.SQLException e) {
      throw new IOException("Failed to generate BLS geographies reference table: " + e.getMessage(), e);
    }

    LOGGER.info("Completed BLS geographies reference table generation");
  }

  /**
   * Generates BLS NAICS sectors reference table from hardcoded NAICS_SUPERSECTORS map.
   */
  private void generateBlsNaicsSectorsReference() throws IOException {
    LOGGER.info("Generating BLS NAICS sectors reference table");

    String parquetPath = storageProvider.resolvePath(parquetDirectory,
        "type=reference/bls_naics_sectors.parquet");

    // Check if already exists
    Map<String, String> params = new HashMap<>();
    params.put("year", String.valueOf(-1));
    CacheKey cacheKey = new CacheKey("reference_bls_naics_sectors", params);

    if (cacheManifest.isParquetConverted(cacheKey) || storageProvider.exists(parquetPath)) {
      LOGGER.info("BLS NAICS sectors reference already exists, skipping");
      return;
    }

    try (java.sql.Connection duckdb = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
      // Create in-memory table with NAICS data
      String createTableSql = "CREATE TABLE bls_naics_sectors ("
          + "supersector_code VARCHAR, "
          + "supersector_name VARCHAR)";

      try (java.sql.Statement stmt = duckdb.createStatement()) {
        stmt.execute(createTableSql);

        // Insert NAICS supersector data
        for (java.util.Map.Entry<String, String> entry : NAICS_SUPERSECTORS.entrySet()) {
          String insertSql = String.format(
              "INSERT INTO bls_naics_sectors VALUES ('%s', '%s')",
              entry.getKey(), entry.getValue().replace("'", "''"));
          stmt.execute(insertSql);
        }

        // Ensure parent directory exists
        ensureParentDirectory(parquetPath);

        // Write parquet file
        String copySql = String.format(
            "COPY (SELECT * FROM bls_naics_sectors ORDER BY supersector_code) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)",
            parquetPath);

        stmt.execute(copySql);

        long count = 0;
        try (java.sql.ResultSet rs = stmt.executeQuery("SELECT count(*) FROM bls_naics_sectors")) {
          if (rs.next()) {
            count = rs.getLong(1);
          }
        }

        LOGGER.info("Generated {} NAICS supersectors to {}", count, parquetPath);

        // Mark as converted in manifest
        cacheManifest.markParquetConverted(cacheKey, parquetPath);
        cacheManifest.save(operatingDirectory);
      }
    } catch (java.sql.SQLException e) {
      throw new IOException("Failed to generate BLS NAICS sectors reference table: " + e.getMessage(), e);
    }

    LOGGER.info("Completed BLS NAICS sectors reference table generation");
  }
}
