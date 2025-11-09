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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
  private final java.util.Set<String> enabledTables;

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

  // QCEW area codes mapping: Publication Code → QCEW Area Code (e.g., "C3562")
  // Format for QCEW Open Data API: "C" + first 4 digits of CBSA code
  // Used to download metro wage data from QCEW Open Data API
  private static final Map<String, String> METRO_QCEW_AREA_CODES = new HashMap<>();
  static {
    METRO_QCEW_AREA_CODES.put("A100", "C3562");  // New York-Newark-Jersey City, NY-NJ-PA MSA
    METRO_QCEW_AREA_CODES.put("A400", "C3108");  // Los Angeles-Long Beach-Anaheim, CA MSA
    METRO_QCEW_AREA_CODES.put("A207", "C1698");  // Chicago-Naperville-Elgin, IL-IN MSA
    METRO_QCEW_AREA_CODES.put("A425", "C2642");  // Houston-The Woodlands-Sugar Land, TX MSA (CBSA 26420)
    METRO_QCEW_AREA_CODES.put("A423", "C3806");  // Phoenix-Mesa-Chandler, AZ MSA
    METRO_QCEW_AREA_CODES.put("A102", "C3798");  // Philadelphia-Camden-Wilmington, PA-NJ-DE-MD MSA
    METRO_QCEW_AREA_CODES.put("A426", "C4170");  // San Antonio-New Braunfels, TX MSA
    METRO_QCEW_AREA_CODES.put("A421", "C4174");  // San Diego-Chula Vista-Carlsbad, CA MSA
    METRO_QCEW_AREA_CODES.put("A127", "C1910");  // Dallas-Fort Worth-Arlington, TX MSA
    METRO_QCEW_AREA_CODES.put("A429", "C4194");  // San Jose-Sunnyvale-Santa Clara, CA MSA
    METRO_QCEW_AREA_CODES.put("A438", "C1242");  // Austin-Round Rock-San Marcos, TX MSA
    METRO_QCEW_AREA_CODES.put("A420", "C2726");  // Jacksonville, FL MSA
    METRO_QCEW_AREA_CODES.put("A103", "C1446");  // Boston-Cambridge-Newton, MA-NH MSA
    METRO_QCEW_AREA_CODES.put("A428", "C4266");  // Seattle-Tacoma-Bellevue, WA MSA
    METRO_QCEW_AREA_CODES.put("A427", "C1974");  // Denver-Aurora-Centennial, CO MSA
    METRO_QCEW_AREA_CODES.put("A101", "C4790");  // Washington-Arlington-Alexandria, DC-VA-MD-WV MSA
    METRO_QCEW_AREA_CODES.put("A211", "C1982");  // Detroit-Warren-Dearborn, MI MSA
    METRO_QCEW_AREA_CODES.put("A104", "C1746");  // Cleveland-Elyria, OH MSA
    METRO_QCEW_AREA_CODES.put("A212", "C3346");  // Minneapolis-St. Paul-Bloomington, MN-WI MSA
    METRO_QCEW_AREA_CODES.put("A422", "C3310");  // Miami-Fort Lauderdale-West Palm Beach, FL MSA
    METRO_QCEW_AREA_CODES.put("A419", "C1206");  // Atlanta-Sandy Springs-Roswell, GA MSA
    METRO_QCEW_AREA_CODES.put("A437", "C3890");  // Portland-Vancouver-Hillsboro, OR-WA MSA
    METRO_QCEW_AREA_CODES.put("A424", "C4014");  // Riverside-San Bernardino-Ontario, CA MSA
    METRO_QCEW_AREA_CODES.put("A320", "C4118");  // St. Louis, MO-IL MSA
    METRO_QCEW_AREA_CODES.put("A319", "C1258");  // Baltimore-Columbia-Towson, MD MSA
    METRO_QCEW_AREA_CODES.put("A433", "C4530");  // Tampa-St. Petersburg-Clearwater, FL MSA
    METRO_QCEW_AREA_CODES.put("A440", "C1126");  // Anchorage, AK MSA
  }

  // Metro area names (publication code -> name)
  // Used for metro_wages table metro_name column
  private static final Map<String, String> METRO_NAMES = new HashMap<>();
  static {
    METRO_NAMES.put("A100", "New York-Newark-Jersey City, NY-NJ-PA");
    METRO_NAMES.put("A400", "Los Angeles-Long Beach-Anaheim, CA");
    METRO_NAMES.put("A207", "Chicago-Naperville-Elgin, IL-IN");
    METRO_NAMES.put("A425", "Houston-The Woodlands-Sugar Land, TX");
    METRO_NAMES.put("A423", "Phoenix-Mesa-Chandler, AZ");
    METRO_NAMES.put("A102", "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD");
    METRO_NAMES.put("A426", "San Antonio-New Braunfels, TX");
    METRO_NAMES.put("A421", "San Diego-Chula Vista-Carlsbad, CA");
    METRO_NAMES.put("A127", "Dallas-Fort Worth-Arlington, TX");
    METRO_NAMES.put("A429", "San Jose-Sunnyvale-Santa Clara, CA");
    METRO_NAMES.put("A438", "Austin-Round Rock-San Marcos, TX");
    METRO_NAMES.put("A420", "Jacksonville, FL");
    METRO_NAMES.put("A103", "Boston-Cambridge-Newton, MA-NH");
    METRO_NAMES.put("A428", "Seattle-Tacoma-Bellevue, WA");
    METRO_NAMES.put("A427", "Denver-Aurora-Centennial, CO");
    METRO_NAMES.put("A101", "Washington-Arlington-Alexandria, DC-VA-MD-WV");
    METRO_NAMES.put("A211", "Detroit-Warren-Dearborn, MI");
    METRO_NAMES.put("A104", "Cleveland-Elyria, OH");
    METRO_NAMES.put("A212", "Minneapolis-St. Paul-Bloomington, MN-WI");
    METRO_NAMES.put("A422", "Miami-Fort Lauderdale-West Palm Beach, FL");
    METRO_NAMES.put("A419", "Atlanta-Sandy Springs-Roswell, GA");
    METRO_NAMES.put("A437", "Portland-Vancouver-Hillsboro, OR-WA");
    METRO_NAMES.put("A424", "Riverside-San Bernardino-Ontario, CA");
    METRO_NAMES.put("A320", "St. Louis, MO-IL");
    METRO_NAMES.put("A319", "Baltimore-Columbia-Towson, MD");
    METRO_NAMES.put("A433", "Tampa-St. Petersburg-Clearwater, FL");
    METRO_NAMES.put("A440", "Anchorage, AK");
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

  public BlsDataDownloader(String apiKey, String cacheDir, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    this(apiKey, cacheDir, cacheDir, cacheDir, cacheStorageProvider, storageProvider, null, null);
  }

  public BlsDataDownloader(String apiKey, String cacheDir, String operatingDirectory, String parquetDirectory, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider, CacheManifest sharedManifest, java.util.Set<String> enabledTables) {
    super(cacheDir, operatingDirectory, parquetDirectory, cacheStorageProvider, storageProvider, sharedManifest);
    this.apiKey = apiKey;
    this.enabledTables = enabledTables;
  }

  @Override protected String getTableName() {
    return "employment_statistics";
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
  @Override
  public void downloadAll(int startYear, int endYear) throws IOException, InterruptedException {
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
  private void downloadAllTables(int startYear, int endYear, java.util.Set<String> enabledTables) throws IOException, InterruptedException {
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
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   * @throws IOException If conversion or file I/O fails
   */
  @Override
  public void convertAll(int startYear, int endYear) throws IOException {
    LOGGER.info("Converting BLS data for years {}-{}", startYear, endYear);

    int convertedCount = 0;
    int skippedCount = 0;

    for (int year = startYear; year <= endYear; year++) {
      Map<String, String> variables = new HashMap<>();
      variables.put("year", String.valueOf(year));
      variables.put("frequency", "monthly");

      // Convert each enabled table
      if (enabledTables == null || enabledTables.contains(TABLE_EMPLOYMENT_STATISTICS)) {
        if (convertTableIfNeeded("employment_statistics", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_INFLATION_METRICS)) {
        if (convertTableIfNeeded("inflation_metrics", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_REGIONAL_CPI)) {
        if (convertTableIfNeeded("regional_cpi", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_METRO_CPI)) {
        if (convertTableIfNeeded("metro_cpi", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_STATE_INDUSTRY)) {
        if (convertTableIfNeeded("state_industry", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_STATE_WAGES)) {
        if (convertTableIfNeeded("state_wages", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_METRO_INDUSTRY)) {
        if (convertTableIfNeeded("metro_industry", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_METRO_WAGES)) {
        if (convertTableIfNeeded("metro_wages", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_JOLTS_REGIONAL)) {
        if (convertTableIfNeeded("jolts_regional", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_COUNTY_WAGES)) {
        if (convertTableIfNeeded("county_wages", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_JOLTS_STATE)) {
        if (convertTableIfNeeded("jolts_state", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }

      if (enabledTables == null || enabledTables.contains(TABLE_WAGE_GROWTH)) {
        if (convertTableIfNeeded("wage_growth", year, variables)) {
          convertedCount++;
        } else {
          skippedCount++;
        }
      }
    }

    LOGGER.info("BLS conversion complete: converted {} tables, skipped {} (up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Helper method to convert a table if needed (checks cache manifest first).
   * Returns true if converted, false if skipped.
   */
  private boolean convertTableIfNeeded(String tableName, int year, Map<String, String> variables)
      throws IOException {
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");
    String parquetPath = storageProvider.resolvePath(parquetDirectory, resolveParquetPath(pattern, variables));
    String rawPath = cacheStorageProvider.resolvePath(cacheDirectory, resolveJsonPath(pattern, variables));

    if (isParquetConvertedOrExists(tableName, year, variables, rawPath, parquetPath)) {
      return false; // Already converted, skip
    }

    convertCachedJsonToParquet(tableName, variables);
    cacheManifest.markParquetConverted(tableName, year, null, parquetPath);
    return true; // Converted
  }

  /**
   * Downloads employment statistics data and converts to Parquet.
   */
  public void downloadEmploymentStatistics(int startYear, int endYear) throws IOException, InterruptedException {

    // Series IDs to fetch (constant across all years)
    List<String> seriesIds =
        List.of(Series.UNEMPLOYMENT_RATE,
        Series.EMPLOYMENT_LEVEL,
        Series.LABOR_FORCE_PARTICIPATION);

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All employment statistics data cached (years {}-{})", startYear, endYear);
      return;
    }

    // 2. Batch fetches uncached years (optimized: up to 20 contiguous years per API call)
    LOGGER.info("Fetching employment statistics for {} uncached years", uncachedYears.size());
    Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, uncachedYears);

    // 3. Save each uncached year
    for (int year : uncachedYears) {
      String outputDirPath = buildPartitionPath("employment_statistics", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/employment_statistics.json";
      Map<String, String> cacheParams = new HashMap<>();

      String rawJson = resultsByYear.get(year);
      if (rawJson != null) {
        validateAndSaveBlsResponse("employment_statistics", year, cacheParams, jsonFilePath, rawJson);
      }
    }

  }

  /**
   * Downloads CPI data for 4 Census regions (Northeast, Midwest, South, West).
   */
  public void downloadRegionalCpi(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading regional CPI for 4 Census regions for {}-{}", startYear, endYear);

    List<String> seriesIds = Series.getAllRegionalCpiSeriesIds();

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = buildPartitionPath("regional_cpi", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/regional_cpi.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("regional_cpi", year, cacheParams)) {
        LOGGER.info("Found cached regional CPI for year {} - skipping", year);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All regional CPI data cached (years {}-{})", startYear, endYear);
      return;
    }

    // 2. Batch fetch uncached years
    LOGGER.info("Fetching regional CPI for {} uncached years", uncachedYears.size());
    Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, uncachedYears);

    // 3. Save each year
    for (int year : uncachedYears) {
      String outputDirPath = buildPartitionPath("regional_cpi", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/regional_cpi.json";
      Map<String, String> cacheParams = new HashMap<>();

      String rawJson = resultsByYear.get(year);
      if (rawJson != null) {
        validateAndSaveBlsResponse("regional_cpi", year, cacheParams, jsonFilePath, rawJson);
      }
    }

  }

  /**
   * Downloads CPI data for 27 major metro areas.
   */
  public void downloadMetroCpi(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading metro area CPI for {} metros for {}-{}",
                METRO_AREA_CODES.size(), startYear, endYear);

    List<String> seriesIds = Series.getAllMetroCpiSeriesIds();
    LOGGER.info("Metro CPI series examples: {}, {}, {}",
        !seriesIds.isEmpty() ? seriesIds.get(0) : "none",
                seriesIds.size() > 1 ? seriesIds.get(1) : "none",
                seriesIds.size() > 2 ? seriesIds.get(2) : "none");

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = buildPartitionPath("metro_cpi", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/metro_cpi.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("metro_cpi", year, cacheParams)) {
        LOGGER.info("Found cached metro CPI for year {} - skipping", year);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All metro CPI data cached (years {}-{})", startYear, endYear);
      return;
    }

    // 2. Batch fetch uncached years
    LOGGER.info("Fetching metro CPI for {} uncached years", uncachedYears.size());
    Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, uncachedYears);

    // 3. Save each year
    for (int year : uncachedYears) {
      String outputDirPath = buildPartitionPath("metro_cpi", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/metro_cpi.json";
      Map<String, String> cacheParams = new HashMap<>();

      String rawJson = resultsByYear.get(year);
      if (rawJson == null) {
        LOGGER.warn("No data returned from API for metro_cpi year {} - skipping save", year);
      } else {
        validateAndSaveBlsResponse("metro_cpi", year, cacheParams, jsonFilePath, rawJson);
      }
    }

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

    List<String> seriesIds = Series.getAllStateIndustryEmploymentSeriesIds();
    LOGGER.info("Generated {} state industry employment series IDs", seriesIds.size());
    LOGGER.info("State industry series examples: {}, {}, {}",
        !seriesIds.isEmpty() ? seriesIds.get(0) : "none",
                seriesIds.size() > 1 ? seriesIds.get(1) : "none",
                seriesIds.size() > 2 ? seriesIds.get(2) : "none");

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String jsonFilePath = buildPartitionPath("state_industry", DataFrequency.QUARTERLY, year) + "/state_industry.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("state_industry", year, cacheParams)) {
        LOGGER.info("Found cached state industry employment for year {} - skipping", year);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All state industry employment data cached (years {}-{})", startYear, endYear);
      return;
    }

    // 2. Batch fetch uncached years (with series batching)
    LOGGER.info("Fetching state industry employment for {} uncached years", uncachedYears.size());
    Map<Integer, String> resultsByYear = fetchAndSplitByYearLargeSeries(seriesIds, uncachedYears);

    // 3. Save each year
    for (int year : uncachedYears) {
      String jsonFilePath = buildPartitionPath("state_industry", DataFrequency.QUARTERLY, year) + "/state_industry.json";
      Map<String, String> cacheParams = new HashMap<>();

      String rawJson = resultsByYear.get(year);
      if (rawJson == null) {
        LOGGER.warn("No data returned from API for state_industry year {} - skipping save", year);
      } else {
        validateAndSaveBlsResponse("state_industry", year, cacheParams, jsonFilePath, rawJson);
      }
    }

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

    for (int year = startYear; year <= endYear; year++) {
      // QCEW data only available from 1990 forward
      if (year < 1990) {
        LOGGER.warn("QCEW data only available from 1990 forward. Skipping year {}", year);
        continue;
      }

      String outputDirPath = buildPartitionPath("state_wages", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/state_wages.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("state_wages", year, cacheParams)) {
        LOGGER.info("Found cached state wages for year {} - skipping", year);
        continue;
      }

      // Download QCEW CSV file
      String qcewZipPath = "type=qcew/year=" + year + "/qcew_annual.zip";
      byte[] zipData = downloadQcewCsvIfNeeded(year, qcewZipPath);

      if (zipData == null) {
        LOGGER.warn("Failed to download QCEW CSV for year {} - skipping state wages", year);
        continue;
      }

      // Parse CSV and extract state-level wage data (agglvl_code = 50)
      String stateWagesJson = parseQcewForStateWages(zipData, year);

      if (stateWagesJson != null) {
        saveToCache("state_wages", year, cacheParams, jsonFilePath, stateWagesJson);
        LOGGER.info("Extracted state wages for year {} ({} states)", year, STATE_FIPS_MAP.size());
      }
    }

  }

  /**
   * Downloads county-level wage data from QCEW annual CSV files and converts to Parquet.
   * Extracts data for ~6,038 counties (most granular wage data available).
   * Reuses the same QCEW CSV files already downloaded for state wages.
   */
  public void downloadCountyWages(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading county wages from QCEW CSV files for {}-{}", startYear, endYear);

    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = buildPartitionPath("county_wages", DataFrequency.QUARTERLY, year);
      String jsonFilePath = outputDirPath + "/county_wages.json";
      Map<String, String> cacheParams = new HashMap<>();

      // Check if a file is already in the cache and up to date
      if (isCachedOrExists("county_wages", year, cacheParams)) {
        LOGGER.info("County wages data for year {} is already cached", year);
        continue;
      }

      // Download QCEW CSV (reuses cache from state_wages if available)
      String qcewZipPath = "type=qcew/year=" + year + "/qcew_annual.zip";
      byte[] zipData = downloadQcewCsvIfNeeded(year, qcewZipPath);

      if (zipData == null) {
        LOGGER.warn("Failed to download QCEW CSV for year {} - skipping county wages", year);
        continue;
      }

      // Parse CSV and extract county-level wage data (agglvl_code = 70)
      String countyWagesJson = parseQcewForCountyWages(zipData, year);

      if (countyWagesJson != null) {
        saveToCache("county_wages", year, cacheParams, jsonFilePath, countyWagesJson);
        LOGGER.info("Extracted county wages for year {}", year);
      }
    }

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

    for (int year = startYear; year <= endYear; year++) {
      String fullParquetPath =
          storageProvider.resolvePath(parquetDirectory, "type=county_qcew/frequency=quarterly/year=" + year + "/county_qcew.parquet");

      // Check if already exists
      if (storageProvider.exists(fullParquetPath)) {
        LOGGER.info("County QCEW data for year {} already exists - skipping", year);
        continue;
      }

      // Download QCEW CSV (reuses cache from state_wages/county_wages if available)
      String qcewZipPath = "type=qcew/year=" + year + "/qcew_annual.zip";
      byte[] zipData = downloadQcewCsvIfNeeded(year, qcewZipPath);

      if (zipData == null) {
        LOGGER.warn("Failed to download QCEW CSV for year {} - skipping county QCEW", year);
        continue;
      }

      // Parse and convert to Parquet
      parseAndConvertQcewToParquet(zipData, fullParquetPath, year);
      LOGGER.info("Completed county QCEW data for year {}", year);
    }

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

    List<String> seriesIds = Series.getAllMetroIndustryEmploymentSeriesIds();
    LOGGER.info("Generated {} metro industry employment series IDs", seriesIds.size());
    LOGGER.info("Metro industry series examples: {}, {}, {}",
        !seriesIds.isEmpty() ? seriesIds.get(0) : "none",
                seriesIds.size() > 1 ? seriesIds.get(1) : "none",
                seriesIds.size() > 2 ? seriesIds.get(2) : "none");

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String jsonFilePath = buildPartitionPath("metro_industry", DataFrequency.QUARTERLY, year) + "/metro_industry.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("metro_industry", year, cacheParams)) {
        LOGGER.info("Found cached metro industry employment for year {} - skipping", year);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All metro industry employment data cached (years {}-{})", startYear, endYear);
      return;
    }

    // 2. Batch fetch uncached years (with series batching)
    LOGGER.info("Fetching metro industry employment for {} uncached years", uncachedYears.size());
    Map<Integer, String> resultsByYear = fetchAndSplitByYearLargeSeries(seriesIds, uncachedYears);

    // 3. Save each year
    for (int year : uncachedYears) {
      String jsonFilePath = buildPartitionPath("metro_industry", DataFrequency.QUARTERLY, year) + "/metro_industry.json";
      Map<String, String> cacheParams = new HashMap<>();

      String rawJson = resultsByYear.get(year);
      if (rawJson == null) {
        LOGGER.warn("No data returned from API for metro_industry year {} - skipping save", year);
      } else {
        validateAndSaveBlsResponse("metro_industry", year, cacheParams, jsonFilePath, rawJson);
      }
    }

  }

  /**
   * Downloads average weekly wages for 27 major U.S. metropolitan areas
   * from BLS QCEW (Quarterly Census of Employment and Wages) bulk CSV files.
   *
   * <p><b>Data Source:</b>
   * Downloads both annual and quarterly bulk CSV files from BLS:
   * <pre>
   * Annual: <a href="https://data.bls.gov/cew/data/files/">...</a>{year}/csv/{year}_annual_singlefile.zip (~80MB)
   * Quarterly: https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip (~323MB)
   * </pre>
   *
   * <p><b>Data Structure:</b>
   * Each record contains:
   * <ul>
   *   <li>{@code metro_area_code} - Metro publication code (e.g., "A419" for Atlanta)</li>
   *   <li>{@code metro_area_name} - Metro area name</li>
   *   <li>{@code year} - Year of data</li>
   *   <li>{@code qtr} - Quarter: "A" for annual average, "1"-"4" for quarterly data</li>
   *   <li>{@code average_weekly_wage} - Average weekly wage (CSV field 14)</li>
   *   <li>{@code average_annual_pay} - Average annual pay (CSV field 15)</li>
   * </ul>
   *
   * <p><b>Cache Structure:</b>
   * <pre>
   * ZIP files: source=econ/type=qcew_bulk/year={year}/{frequency}_singlefile.zip
   * Extracted CSV: source=econ/type=qcew_bulk/year={year}/{year}.{frequency}.singlefile.csv
   * JSON output: source=econ/type=metro_wages/frequency=monthly/year={year}/metro_wages.json
   * </pre>
   *
   * <p><b>Coverage:</b>
   * 27 major metropolitan areas (defined in {@code METRO_QCEW_AREA_CODES}) with both
   * annual and quarterly data from 1990 to present.
   *
   * @param startYear Start year (must be >= 1990, when QCEW data begins)
   * @param endYear   End year
   * @throws IOException If download or file operations fail
   * @see #downloadQcewMetroWagesFromApi(int) For implementation details
   * @see #downloadQcewBulkFile(int, String) For bulk file download logic
   * @see #parseQcewBulkFile(String, java.util.Set) For CSV parsing and filtering logic
   */
  public void downloadMetroWages(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading metro wages from QCEW Open Data API for {}-{}", startYear, endYear);

    for (int year = startYear; year <= endYear; year++) {
      // QCEW data only available from 1990 forward
      if (year < 1990) {
        LOGGER.warn("QCEW data only available from 1990 forward. Skipping year {}", year);
        continue;
      }

      String jsonFilePath = buildPartitionPath("metro_wages", DataFrequency.MONTHLY, year) + "/metro_wages.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("metro_wages", year, cacheParams)) {
        LOGGER.info("Found cached metro wages for year {} - skipping", year);
        continue;
      }

      // Download quarterly data from QCEW Open Data API for all 27 metros
      String metroWagesJson = downloadQcewMetroWagesFromApi(year);

      if (metroWagesJson != null) {
        saveToCache("metro_wages", year, cacheParams, jsonFilePath, metroWagesJson);
        LOGGER.info("Downloaded metro wages for year {} ({} metros)", year, METRO_QCEW_AREA_CODES.size());
      } else {
        LOGGER.warn("Failed to download metro wages for year {}", year);
      }
    }

  }

  /**
   * Downloads metro wage data from QCEW bulk CSV files for a specific year.
   * Uses bulk download approach instead of per-metro API calls to avoid HTTP 404 errors.
   *
   * <p>Downloads both annual and quarterly bulk files, extracts data for all 27 metros,
   * and returns combined records with qtr field ("A" for annual, "1"-"4" for quarters).
   *
   * @param year The year to download data for
   * @return JSON array string containing metro wage records or null if download fails
   */
  private String downloadQcewMetroWagesFromApi(int year) {
    LOGGER.info("Downloading QCEW metro wages from bulk files for {} metros in year {}",
        METRO_QCEW_AREA_CODES.size(), year);

    List<Map<String, Object>> allMetroWages = new ArrayList<>();

    try {
      // Download and extract bulk files (annual + quarterly)
      String annualZipPath = downloadQcewBulkFile(year, "annual");
      String annualCsvPath = extractQcewBulkFile(annualZipPath, year, "annual");

      String qtrlyZipPath = downloadQcewBulkFile(year, "qtrly");
      String qtrlyCsvPath = extractQcewBulkFile(qtrlyZipPath, year, "qtrly");

      // Build a set of C-codes to extract from bulk files
      java.util.Set<String> metroCCodes = new java.util.HashSet<>(METRO_QCEW_AREA_CODES.values());

      // Parse annual data (qtr="A")
      List<MetroWageRecord> annualRecords = parseQcewBulkFile(annualCsvPath, metroCCodes);
      LOGGER.info("Extracted {} annual metro wage records from bulk file", annualRecords.size());

      // Parse quarterly data (qtr="1","2","3","4")
      List<MetroWageRecord> qtrlyRecords = parseQcewBulkFile(qtrlyCsvPath, metroCCodes);
      LOGGER.info("Extracted {} quarterly metro wage records from bulk file", qtrlyRecords.size());

      // Combine annual + quarterly records and convert to JSON format
      List<MetroWageRecord> allRecords = new ArrayList<>();
      allRecords.addAll(annualRecords);
      allRecords.addAll(qtrlyRecords);

      for (MetroWageRecord record : allRecords) {
        Map<String, Object> wageData = new HashMap<>();
        wageData.put("metro_area_code", record.metroCode);
        wageData.put("metro_area_name", record.metroName);
        wageData.put("year", record.year);
        wageData.put("qtr", record.qtr);

        if (record.avgWklyWage != null) {
          wageData.put("average_weekly_wage", record.avgWklyWage);
        }
        if (record.avgAnnualPay != null) {
          wageData.put("average_annual_pay", record.avgAnnualPay);
        }

        allMetroWages.add(wageData);
      }

      if (allMetroWages.isEmpty()) {
        LOGGER.error("No metro wage data extracted from bulk files for year {}", year);
        return null;
      }

      LOGGER.info("Successfully processed {} metro wage records (annual + quarterly) for year {}",
          allMetroWages.size(), year);

      // Convert to JSON array format
      return MAPPER.writeValueAsString(allMetroWages);

    } catch (Exception e) {
      LOGGER.error("Failed to download/parse metro wages from bulk files for year {}: {}",
          year, e.getMessage(), e);
      return null;
    }
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
   * Extracts QCEW bulk ZIP file to a cached CSV file.
   *
   * <p>Extracts the single CSV file contained in the QCEW bulk ZIP file to a cached location
   * to avoid re-extraction on subsequent runs. For large files (500MB+ uncompressed), this
   * disk-based caching is more efficient than in-memory processing.
   *
   * <p>Extracted CSV cached at: a source=econ/type=qcew_bulk/{year}/{year}.{frequency}.singlefile.csv
   *
   * @param zipFilePath Path to a cached ZIP file (from downloadQcewBulkFile)
   * @param year Year of the data
   * @param frequency Frequency type: "annual" or "qtrly"
   * @return Path to an extracted CSV file
   * @throws IOException if extraction fails
   */
  String extractQcewBulkFile(String zipFilePath, int year, String frequency) throws IOException {
    // Build cache path for extracted CSV: source=econ/type=qcew_bulk/{year}/{year}.{frequency}.singlefile.csv
    String csvRelativePath =
                                            String.format("type=qcew_bulk/year=%d/%d.%s.singlefile.csv", year, year, frequency);
    String csvFullPath = cacheStorageProvider.resolvePath(cacheDirectory, csvRelativePath);

    // Check if CSV already extracted
    if (cacheStorageProvider.exists(csvFullPath)) {
      LOGGER.info("Using cached extracted QCEW {} CSV for {}: {}", frequency, year, csvRelativePath);
      return csvFullPath;
    }

    LOGGER.info("Extracting QCEW bulk {} file for {} to cache", frequency, year);

    try (InputStream zipIn = cacheStorageProvider.openInputStream(zipFilePath);
         ZipInputStream zis = new ZipInputStream(new java.io.BufferedInputStream(zipIn))) {

      ZipEntry entry = zis.getNextEntry();
      if (entry == null) {
        throw new IOException("ZIP file contains no entries: " + zipFilePath);
      }
      if (entry.isDirectory()) {
        throw new IOException("Unexpected directory entry in ZIP: " + entry.getName());
      }

      LOGGER.debug("Extracting ZIP entry: {} (compressed: {} bytes, uncompressed: {} bytes)",
                   entry.getName(), entry.getCompressedSize(), entry.getSize());

      // Ensure destination directory exists (if applicable for this storage provider)
      try {
        java.nio.file.Path parent = java.nio.file.Paths.get(csvFullPath).getParent();
        if (parent != null) {
          cacheStorageProvider.createDirectories(parent.toString());
        }
      } catch (Exception ignore) {
        // Some providers may not require directory creation
      }

      // Stream the CSV entry directly to the cache without buffering entire file in memory
      cacheStorageProvider.writeFile(csvFullPath, new java.io.BufferedInputStream(zis, 1 << 20));
      zis.closeEntry();

      LOGGER.info("Extracted and cached QCEW {} CSV for {}.", frequency, year);
      return csvFullPath;

    } catch (IOException e) {
      LOGGER.error("Error extracting ZIP file {}: {}", zipFilePath, e.getMessage());
      throw new IOException("Failed to extract QCEW bulk file: " + e.getMessage(), e);
    }
  }

  /**
   * Data class for holding metro wage records extracted from QCEW bulk files.
   */
  static class MetroWageRecord {
    final String metroCode;      // Publication code (e.g., "A419" for Atlanta)
    final String metroName;      // Metro area name
    final int year;              // Year
    final String qtr;            // Quarter: "1", "2", "3", "4", or "A" for annual
    final Integer avgWklyWage;   // Average weekly wage (field 14)
    final Integer avgAnnualPay;  // Average annual pay (field 15)

    MetroWageRecord(String metroCode, String metroName, int year, String qtr,
                    Integer avgWklyWage, Integer avgAnnualPay) {
      this.metroCode = metroCode;
      this.metroName = metroName;
      this.year = year;
      this.qtr = qtr;
      this.avgWklyWage = avgWklyWage;
      this.avgAnnualPay = avgAnnualPay;
    }
  }

  /**
   * Parses QCEW bulk CSV file and extracts metro wage data for specified metro areas.
   *
   * <p>Bulk CSV files contain ALL metro areas. This method filters rows for the specified
   * metro C-codes and extracts wage data. Uses stream processing to handle large files (500MB+).
   *
   * <p>Filtering criteria:
   * - area_fips IN metroC-codes (e.g., "C1206", "C1242", "C3890")
   * - own_code = "0" (total, all ownership)
   * - industry_code = "10" (total, all industries)
   * - agglvl_code = "80" (MSA level, not "40," which is county)
   *
   * <p>Extracts fields:
   * - Field 6: year
   * - Field 7: qtr ("1", "2", "3", "4", or "A" for annual)
   * - Field 14: annual_avg_wkly_wage
   * - Field 15: avg_annual_pay
   *
   * @param csvFilePath Path to an extracted CSV file (from extractQcewBulkFile)
   * @param metroCCodes Set of QCEW C-codes to extract (e.g., {"C1206", "C1242", "C3890"})
   * @return List of metro wage records extracted from the CSV
   * @throws IOException if reading or parsing fails
   */
  List<MetroWageRecord> parseQcewBulkFile(String csvFilePath, java.util.Set<String> metroCCodes)
      throws IOException {
    List<MetroWageRecord> records = new ArrayList<>();

    LOGGER.info("Parsing QCEW bulk CSV file for {} metro areas: {}", metroCCodes.size(), csvFilePath);

    long linesProcessed = 0;
    long matchedRows = 0;

    try (InputStream csvInputStream = cacheStorageProvider.openInputStream(csvFilePath);
         BufferedReader reader =
             new BufferedReader(new InputStreamReader(csvInputStream, StandardCharsets.UTF_8))) {

      String line;
      boolean isHeader = true;

      while ((line = reader.readLine()) != null) {
        linesProcessed++;

        // Log progress every 100K lines
        if (linesProcessed % 100000 == 0) {
          LOGGER.debug("Processed {} lines, found {} matching metro records", linesProcessed, matchedRows);
        }

        if (isHeader) {
          isHeader = false;
          continue;
        }

        String[] fields = parseCsvLine(line);
        if (fields.length < 16) continue;  // Need at least 16 fields

        String areaFips = fields[0].trim();
        String ownCode = fields[1].trim();
        String industryCode = fields[2].trim();
        String agglvlCode = fields[3].trim();

        // Filter: MSA level (80), all ownership (0), total industry (10), and metro C-code
        if (!agglvlCode.equals("80") || !ownCode.equals("0") || !industryCode.equals("10")) {
          continue;
        }

        if (!metroCCodes.contains(areaFips)) {
          continue;
        }

        // Found a matching metro record - extract fields
        matchedRows++;

        try {
          int year = Integer.parseInt(fields[5].trim());
          String qtr = fields[6].trim();
          Integer avgWklyWage = parseIntOrNull(fields[14]);
          Integer avgAnnualPay = parseIntOrNull(fields[15]);

          // Map C-code to publication code and metro name
          String publicationCode = getMetroPublicationCode(areaFips);
          String metroName = getMetroName(publicationCode);

          if (publicationCode != null && metroName != null) {
            MetroWageRecord record =
                new MetroWageRecord(publicationCode, metroName, year, qtr, avgWklyWage, avgAnnualPay);
            records.add(record);
          } else {
            LOGGER.warn("Could not map C-code {} to publication code/name", areaFips);
          }

        } catch (NumberFormatException e) {
          LOGGER.warn("Error parsing numeric fields in line {}: {}", linesProcessed, e.getMessage());
        }
      }

      LOGGER.info("Completed parsing QCEW bulk CSV: processed {} lines, found {} metro wage records",
                  linesProcessed, matchedRows);

    } catch (IOException e) {
      LOGGER.error("Error reading CSV file {}: {}", csvFilePath, e.getMessage());
      throw new IOException("Failed to parse QCEW bulk CSV file: " + e.getMessage(), e);
    }

    return records;
  }

  /**
   * Maps a QCEW C-code to the metro publication code.
   * Uses reverse lookup in METRO_QCEW_AREA_CODES map.
   *
   * @param cCode QCEW C-code (e.g., "C1206")
   * @return Publication code (e.g., "A419"), or null if not found
   */
  private String getMetroPublicationCode(String cCode) {
    for (Map.Entry<String, String> entry : METRO_QCEW_AREA_CODES.entrySet()) {
      if (entry.getValue().equals(cCode)) {
        return entry.getKey();
      }
    }
    return null;
  }

  /**
   * Gets the metro area name for a publication code.
   *
   * @param publicationCode Metro publication code (e.g., "A419")
   * @return Metro area name, or null if not found
   */
  private String getMetroName(String publicationCode) {
    return METRO_NAMES.get(publicationCode);
  }

  /**
   * Downloads JOLTS (Job Openings and Labor Turnover Survey) regional data from BLS FTP flat files.
   * Regional data is NOT available via BLS API v2 - must use download.bls.gov flat files.
   * Covers 4 Census regions (Northeast, Midwest, South, West) with 5 metrics each (20 series).
   */
  public void downloadJoltsRegional(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading JOLTS regional data from BLS FTP flat files for {}-{}", startYear, endYear);

    for (int year = startYear; year <= endYear; year++) {
      if (year < 2001) {
        LOGGER.warn("JOLTS data only available from 2001 forward. Skipping year {}", year);
        continue;
      }

      String jsonFilePath = buildPartitionPath("jolts_regional", DataFrequency.MONTHLY, year) + "/jolts_regional.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("jolts_regional", year, cacheParams)) {
        LOGGER.info("Found cached JOLTS regional for year {} - skipping", year);
        continue;
      }

      String joltsFtpPath = "type=jolts_ftp/jolts_series.txt";
      byte[] seriesData = downloadJoltsFtpFileIfNeeded(joltsFtpPath, "https://download.bls.gov/pub/time.series/jt/jt.series");

      if (seriesData == null) {
        LOGGER.warn("Failed to download JOLTS series file - skipping year {}", year);
        continue;
      }

      String joltsRegionalJson = parseJoltsFtpForRegional(year);

      if (joltsRegionalJson != null) {
        String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonFilePath);
        saveToCache("jolts_regional", year, cacheParams, jsonFilePath, joltsRegionalJson);
        LOGGER.info("Extracted JOLTS regional data for year {} (4 regions × 5 metrics)", year);
      }
    }

  }

  /**
   * Downloads JOLTS state-level data from BLS FTP flat files and converts to Parquet.
   * Extracts data for all 51 states (including DC) for 5 metrics (job openings, hires, separations, quits, layoffs).
   */
  public void downloadJoltsState(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading JOLTS state data from BLS FTP flat files for {}-{}", startYear, endYear);

    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = buildPartitionPath("jolts_state", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/jolts_state.json";
      Map<String, String> cacheParams = new HashMap<>();

      // Check if a file is already in the cache and up to date
      if (isCachedOrExists("jolts_state", year, cacheParams)) {
        LOGGER.info("JOLTS state data for year {} is already cached", year);
        continue;
      }

      // Parse JOLTS FTP files for state data
      String joltsStateJson = parseJoltsFtpForState(year);

      if (joltsStateJson != null) {
        saveToCache("jolts_state", year, cacheParams, jsonFilePath, joltsStateJson);
        LOGGER.info("Extracted JOLTS state data for year {} (51 states × 5 metrics)", year);
      }
    }

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

    // Check if already cached (use year=-1 for non-partitioned reference data)
    if (isCachedOrExists("jolts_industries", -1, cacheParams)) {
      LOGGER.info("JOLTS industry reference data already cached");
      return;
    }

    String url = "https://download.bls.gov/pub/time.series/jt/jt.industry";
    String ftpPath = "type=jolts_ftp/jt.industry";

    // Download file (will be cached by downloadJoltsFtpFileIfNeeded)
    byte[] data = downloadJoltsFtpFileIfNeeded(ftpPath, url);
    if (data == null) {
      LOGGER.warn("Failed to download JOLTS industry reference file");
      return;
    }

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
    saveToCache("jolts_industries", -1, cacheParams, jsonFilePath, json);
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

    // Check if already cached (use year=-1 for non-partitioned reference data)
    if (isCachedOrExists("jolts_dataelements", -1, cacheParams)) {
      LOGGER.info("JOLTS data element reference data already cached");
      return;
    }

    String url = "https://download.bls.gov/pub/time.series/jt/jt.dataelement";
    String ftpPath = "type=jolts_ftp/jt.dataelement";

    // Download file (will be cached by downloadJoltsFtpFileIfNeeded)
    byte[] data = downloadJoltsFtpFileIfNeeded(ftpPath, url);
    if (data == null) {
      LOGGER.warn("Failed to download JOLTS data element reference file");
      return;
    }

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
    saveToCache("jolts_dataelements", -1, cacheParams, jsonFilePath, json);
  }

  /**
   * Downloads inflation metrics data and converts to Parquet.
   */
  public void downloadInflationMetrics(int startYear, int endYear) throws IOException, InterruptedException {

    List<String> seriesIds =
        List.of(Series.CPI_ALL_URBAN,
        Series.CPI_CORE,
        Series.PPI_FINAL_DEMAND);

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = buildPartitionPath("inflation_metrics", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/inflation_metrics.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("inflation_metrics", year, cacheParams)) {
        LOGGER.info("Found cached inflation metrics for year {} - skipping", year);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All inflation metrics data cached (years {}-{})", startYear, endYear);
      return;
    }

    // 2. Batch fetch uncached years
    LOGGER.info("Fetching inflation metrics for {} uncached years", uncachedYears.size());
    Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, uncachedYears);

    // 3. Save each year
    for (int year : uncachedYears) {
      String outputDirPath = buildPartitionPath("inflation_metrics", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/inflation_metrics.json";
      Map<String, String> cacheParams = new HashMap<>();

      String rawJson = resultsByYear.get(year);
      if (rawJson != null) {
        validateAndSaveBlsResponse("inflation_metrics", year, cacheParams, jsonFilePath, rawJson);
      }
    }

  }

  /**
   * Downloads wage growth data and converts to Parquet.
   */
  public void downloadWageGrowth(int startYear, int endYear) throws IOException, InterruptedException {

    List<String> seriesIds =
        List.of(Series.AVG_HOURLY_EARNINGS,
        Series.EMPLOYMENT_COST_INDEX);

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String relativePath = buildPartitionPath("wage_growth", DataFrequency.QUARTERLY, year) + "/wage_growth.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("wage_growth", year, cacheParams)) {
        LOGGER.info("Found cached wage growth data for year {} - skipping", year);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All wage growth data cached (years {}-{})", startYear, endYear);
      return;
    }

    // 2. Batch fetch uncached years
    LOGGER.info("Fetching wage growth for {} uncached years", uncachedYears.size());
    Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, uncachedYears);

    // 3. Save each year
    for (int year : uncachedYears) {
      String relativePath = buildPartitionPath("wage_growth", DataFrequency.QUARTERLY, year) + "/wage_growth.json";
      Map<String, String> cacheParams = new HashMap<>();

      String rawJson = resultsByYear.get(year);
      if (rawJson != null) {
        validateAndSaveBlsResponse("wage_growth", year, cacheParams, relativePath, rawJson);
      }
    }

  }

  /**
   * Downloads state-level LAUS (Local Area Unemployment Statistics) data for all 51 jurisdictions
   * (50 states + DC). Includes unemployment rate, employment level, unemployment level, and labor force.
   *
   * <p>Data is partitioned by year and state_fips, with each state saved to a separate parquet file.
   * This enables incremental downloads - if a state file already exists for a given year, it's skipped.
   *
   * <p>Optimized to batch up to 20 years per API call (per state), reducing total API calls from
   * ~1,275 (51 states × 25 years) to ~102 (51 states × ~2 batches).
   */
  public void downloadRegionalEmployment(int startYear, int endYear) throws IOException {
    LOGGER.info("Downloading regional employment data for all 51 states/jurisdictions (years {}-{})", startYear, endYear);

    int totalFilesDownloaded = 0;
    int totalFilesSkipped = 0;

    // Process each state independently with year-batching
    for (Map.Entry<String, String> entry : STATE_FIPS_MAP.entrySet()) {
      String stateName = entry.getKey();
      String stateFips = entry.getValue();

      LOGGER.info("Processing state {} (FIPS {})", stateName, stateFips);

      // 1. Identify uncached years for this state
      List<Integer> uncachedYears = new ArrayList<>();
      for (int year = startYear; year <= endYear; year++) {
        String relativeParquetPath = "type=regional/year=" + year + "/state_fips=" + stateFips + "/regional_employment.parquet";
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, relativeParquetPath);

        Map<String, String> cacheParams = new HashMap<>();
        cacheParams.put("state_fips", stateFips);

        // Check the cache manifest first
        if (cacheManifest.isParquetConverted("regional_employment", year, cacheParams)) {
          LOGGER.debug("State {} year {} already cached - skipping", stateName, year);
          totalFilesSkipped++;
          continue;
        }

        // Defensive check: if a file exists but not in manifest, update manifest
        if (storageProvider.exists(fullParquetPath)) {
          LOGGER.info("State {} year {} parquet exists, updating manifest", stateName, year);
          cacheManifest.markParquetConverted("regional_employment", year, cacheParams, relativeParquetPath);
          cacheManifest.save(operatingDirectory);
          totalFilesSkipped++;
          continue;
        }

        uncachedYears.add(year);
      }

      if (uncachedYears.isEmpty()) {
        LOGGER.info("All years cached for state {} - skipping", stateName);
        continue;
      }

      // 2. Generate series IDs for this state (4 measures)
      // Format: LASST{state_fips}0000000000{measure}
      List<String> seriesIds = new ArrayList<>();
      seriesIds.add("LASST" + stateFips + "0000000000003"); // unemployment rate
      seriesIds.add("LASST" + stateFips + "0000000000004"); // unemployment level
      seriesIds.add("LASST" + stateFips + "0000000000005"); // employment level
      seriesIds.add("LASST" + stateFips + "0000000000006"); // labor force

      // 3. Batch fetches uncached years (up to 20 years per API call)
      LOGGER.info("Fetching {} uncached years for state {}", uncachedYears.size(), stateName);
      Map<Integer, String> resultsByYear;
      try {
        resultsByYear = fetchAndSplitByYear(seriesIds, uncachedYears);
      } catch (Exception e) {
        LOGGER.warn("Failed to fetch data for state {}: {}", stateName, e.getMessage());

        // Check if rate limit error
        if (e.getMessage() != null && e.getMessage().contains("rate limit")) {
          LOGGER.warn("BLS API rate limit reached. Stopping download.");
          LOGGER.info("Downloaded {} files, skipped {} already cached", totalFilesDownloaded, totalFilesSkipped);
          return;
        }

        continue; // Skip this state
      }

      // 4. Save each year
      for (int year : uncachedYears) {
        String relativeParquetPath = "type=regional/year=" + year + "/state_fips=" + stateFips + "/regional_employment.parquet";
        String fullParquetPath = storageProvider.resolvePath(parquetDirectory, relativeParquetPath);

        String rawJson = resultsByYear.get(year);
        if (rawJson == null) {
          LOGGER.warn("No data returned for state {} year {} - skipping", stateName, year);
          continue;
        }

        try {
          // Parse and validate response
          JsonNode batchRoot = MAPPER.readTree(rawJson);
          String status = batchRoot.path("status").asText("UNKNOWN");

          if (!"REQUEST_SUCCEEDED".equals(status)) {
            LOGGER.warn("API error for state {} year {}: {} - skipping", stateName, year, status);
            continue;
          }

          JsonNode seriesNode = batchRoot.path("Results").path("series");
          if (!seriesNode.isArray() || seriesNode.isEmpty()) {
            LOGGER.warn("No series data for state {} year {} - skipping", stateName, year);
            continue;
          }

          // Convert JSON response to Parquet and save
          convertAndSaveRegionalEmployment(batchRoot, fullParquetPath, stateFips);

          // Mark as converted in manifest
          Map<String, String> cacheParams = new HashMap<>();
          cacheParams.put("state_fips", stateFips);
          cacheManifest.markParquetConverted("regional_employment", year, cacheParams, relativeParquetPath);
          cacheManifest.save(operatingDirectory);

          totalFilesDownloaded++;

          LOGGER.info("Saved state {} year {} ({} series)", stateName, year, seriesNode.size());

        } catch (Exception e) {
          LOGGER.warn("Failed to save state {} year {}: {}", stateName, year, e.getMessage());
          // Continue with next year
        }
      }
    }

    LOGGER.info("Regional employment download complete: {} files downloaded, {} already cached",
                totalFilesDownloaded, totalFilesSkipped);

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
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();

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
        java.util.Map<String, Object> record = new java.util.HashMap<>();
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
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("regional_employment");
    storageProvider.writeAvroParquet(fullParquetPath, columns, dataRecords, "regional_employment", "regional_employment");

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
   * @param zipData Downloaded ZIP file bytes
   * @param fullParquetPath Full path for the output Parquet file (already resolved)
   * @param year Year of the data
   * @throws IOException if parsing or conversion fails
   */
  private void parseAndConvertQcewToParquet(byte[] zipData, String fullParquetPath, int year) throws IOException {
    LOGGER.info("Parsing QCEW CSV for year {} and converting to Parquet", year);

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    int recordCount = 0;
    int countyRecordCount = 0;

    // Unzip and parse CSV
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipData))) {
      ZipEntry entry;

      while ((entry = zis.getNextEntry()) != null) {
        // Look for CSV files in the ZIP
        if (entry.getName().endsWith(".csv")) {
          LOGGER.info("Processing CSV file: {}", entry.getName());

          // Don't use try-with-resources for BufferedReader as it would close the underlying ZipInputStream
          BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8));
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
                if (recordCount <= 5) {
                  LOGGER.info("DEBUG county_qcew: Skipping malformed record {} with only {} fields", recordCount, fields.length);
                }
                continue; // Skip malformed records
              }

              try {
                String areaFips = fields[0].trim();
                String ownCode = fields[1].trim();
                String industryCode = fields[2].trim();
                String agglvlCode = fields[3].trim();

                // Log the first few records for debugging
                if (recordCount <= 5) {
                  LOGGER.info("DEBUG county_qcew: Record {}: areaFips='{}', ownCode='{}', industryCode='{}', agglvlCode='{}'",
                      recordCount, areaFips, ownCode, industryCode, agglvlCode);
                }

                // Filter to county-level data only (agglvl 70-78 are county aggregations)
                // 70 = County, Total
                // 71-78 = Various county-level industry aggregations
                if (!agglvlCode.startsWith("7")) {
                  if (recordCount <= 10 && !agglvlCode.isEmpty()) {
                    LOGGER.info("DEBUG county_qcew: Skipping record {} - agglvlCode '{}' doesn't start with '7'", recordCount, agglvlCode);
                  }
                  continue;
                }

                // Filter to actual counties (5-digit FIPS codes, not state or national)
                if (areaFips.length() != 5 || areaFips.equals("US000")) {
                  if (countyRecordCount < 5) {
                    LOGGER.info("DEBUG county_qcew: Skipping record {} - areaFips '{}' is not 5 digits or is US000", recordCount, areaFips);
                  }
                  continue;
                }

                countyRecordCount++;

                if (countyRecordCount <= 5) {
                  LOGGER.info("DEBUG county_qcew: Accepted county record {}: areaFips='{}', agglvlCode='{}'", countyRecordCount, areaFips, agglvlCode);
                }

                // Parse numeric fields (handle empty values)
                Integer avgEstabs = parseIntOrNull(fields[13]);
                Integer avgEmplvl = parseIntOrNull(fields[14]);
                Long totalWages = parseLongOrNull(fields[15]);
                Integer avgWklyWage = parseIntOrNull(fields[18]);

                // Create a data record
                java.util.Map<String, Object> record = new java.util.HashMap<>();
                record.put("area_fips", areaFips);
                record.put("own_code", ownCode);
                record.put("industry_code", industryCode);
                record.put("agglvl_code", agglvlCode);
                record.put("annual_avg_estabs", avgEstabs);
                record.put("annual_avg_emplvl", avgEmplvl);
                record.put("total_annual_wages", totalWages);
                record.put("annual_avg_wkly_wage", avgWklyWage);

                dataRecords.add(record);

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
    }

    LOGGER.info("Parsed {} county records from {} total QCEW records", countyRecordCount, recordCount);

    // Load column metadata and write parquet
    if (!dataRecords.isEmpty()) {
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
          loadTableColumns("county_qcew");
      storageProvider.writeAvroParquet(fullParquetPath, columns, dataRecords, "CountyQcew", "CountyQcew");
      LOGGER.info("Converted QCEW data to Parquet: {} ({} records)", fullParquetPath, dataRecords.size());
    } else {
      LOGGER.warn("No county records found in QCEW data for year {}", year);
    }
  }

  /**
   * Downloads QCEW CSV file if not already cached.
   * Reuses cached data if available to avoid redundant downloads.
   *
   * @param year Year to download
   * @param qcewZipPath Relative path for caching
   * @return ZIP file bytes, or null if download fails
   */
  private byte[] downloadQcewCsvIfNeeded(int year, String qcewZipPath) throws IOException {
    // Check the cache manifest first
    Map<String, String> cacheParams = new HashMap<>();
    if (cacheManifest.isCached("qcew_zip", year, cacheParams)) {
      String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);
      if (cacheStorageProvider.exists(fullPath)) {
        LOGGER.info("Using cached QCEW CSV for year {} (from manifest)", year);
        try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(fullPath)) {
          return inputStream.readAllBytes();
        }
      } else {
        LOGGER.warn("Cache manifest lists QCEW ZIP for year {} but file not found - re-downloading", year);
      }
    }

    // Download from BLS
    String url =
        String.format("https://data.bls.gov/cew/data/files/%d/csv/%d_annual_singlefile.zip", year, year);

    LOGGER.info("Downloading QCEW CSV for year {} from {}", year, url);
    byte[] zipData = blsDownloadFile(url);

    // Cache for reuse - use cacheStorageProvider for intermediate files
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);
    cacheStorageProvider.writeFile(fullPath, zipData);

    // Mark in cache manifest - QCEW data is immutable (historical), never refresh
    long refreshAfter = Long.MAX_VALUE;
    ((CacheManifest) cacheManifest).markCached("qcew_zip", year, cacheParams, qcewZipPath, zipData.length, refreshAfter, "immutable_historical");
    cacheManifest.save(operatingDirectory);

    LOGGER.info("Downloaded and cached QCEW CSV for year {} ({} MB)", year, zipData.length / (1024 * 1024));

    return zipData;
  }

  /**
   * Parses QCEW CSV and extracts state-level wage data.
   * Filters for agglvl_code = 50 (state level), own_code = 0 (all ownership),
   * industry_code = 10 (total all industries).
   *
   * @param zipData QCEW CSV ZIP file bytes
   * @param year Year of data
   * @return JSON string in BLS API response format, or null if parsing fails
   */
  private String parseQcewForStateWages(byte[] zipData, int year) throws IOException {
    Map<String, Map<String, Object>> stateWages = new HashMap<>();

    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipData))) {
      ZipEntry entry;

      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().endsWith(".csv")) {
          // Don't use try-with-resources for BufferedReader - it would close the ZipInputStream
          BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8));
          String line;
          boolean isHeader = true;

          while ((line = reader.readLine()) != null) {
            if (isHeader) {
              isHeader = false;
              continue;
            }

            String[] fields = parseCsvLine(line);
            if (fields.length < 20) continue;

            String areaFips = fields[0].trim();
            String ownCode = fields[1].trim();
            String industryCode = fields[2].trim();
            String agglvlCode = fields[3].trim();

            // Filter: state level (50), all ownership (0), total industry (10)
            if (!agglvlCode.equals("50") || !ownCode.equals("0") || !industryCode.equals("10")) {
              continue;
            }

            // State FIPS should be 2 digits + "000"
            if (areaFips.length() != 5 || !areaFips.endsWith("000")) {
              continue;
            }

            String stateFips = areaFips.substring(0, 2);
            String stateName = Series.getStateName(stateFips);

            // QCEW CSV fields: [13]=annual_avg_wkly_wage, [9]=annual_avg_emplvl
            Integer avgWklyWage = parseIntOrNull(fields[13]);
            Integer avgEmplvl = parseIntOrNull(fields[9]);

            if (avgWklyWage != null) {
              Map<String, Object> wageData = new HashMap<>();
              wageData.put("state_fips", stateFips);
              wageData.put("state_name", stateName);
              wageData.put("average_weekly_wage", avgWklyWage);
              wageData.put("total_employment", avgEmplvl);
              wageData.put("year", year);

              stateWages.put(stateFips, wageData);
            }
          }
          zis.closeEntry();
        }
      }
    }

    if (stateWages.isEmpty()) {
      LOGGER.warn("No state wage data found in QCEW CSV for year {}", year);
      return null;
    }

    LOGGER.info("Extracted wage data for {} states from QCEW CSV (year {})", stateWages.size(), year);

    // Convert to JSON array format
    try {
      return MAPPER.writeValueAsString(new ArrayList<>(stateWages.values()));
    } catch (Exception e) {
      LOGGER.error("Failed to serialize state wages to JSON: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Parses QCEW CSV and extracts county-level wage data.
   * Filters for agglvl_code = 70 (county level), own_code = 0, industry_code = 10.
   * Most granular wage data available (~6,038 counties).
   *
   * @param zipData QCEW CSV ZIP file bytes
   * @param year Year of data
   * @return JSON string in BLS API response format, or null if parsing fails
   */
  private String parseQcewForCountyWages(byte[] zipData, int year) throws IOException {
    Map<String, Map<String, Object>> countyWages = new HashMap<>();

    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipData))) {
      ZipEntry entry;

      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().endsWith(".csv")) {
          // Don't use try-with-resources for BufferedReader - it would close the ZipInputStream
          BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8));
          String line;
          boolean isHeader = true;

          while ((line = reader.readLine()) != null) {
            if (isHeader) {
              isHeader = false;
              continue;
            }

            String[] fields = parseCsvLine(line);
            if (fields.length < 20) continue;

            String areaFips = fields[0].trim();
            String ownCode = fields[1].trim();
            String industryCode = fields[2].trim();
            String agglvlCode = fields[3].trim();

            // Filter: county level (70), all ownership (0), total industry (10)
            if (!agglvlCode.equals("70") || !ownCode.equals("0") || !industryCode.equals("10")) {
              continue;
            }

            // County FIPS should be 5 digits: state (2) + county (3)
            if (areaFips.length() != 5) {
              continue;
            }

            String stateFips = areaFips.substring(0, 2);
            String stateName = Series.getStateName(stateFips);

            // QCEW CSV fields: [13]=annual_avg_wkly_wage, [9]=annual_avg_emplvl, [8]=area_title
            Integer avgWklyWage = parseIntOrNull(fields[13]);
            Integer avgEmplvl = parseIntOrNull(fields[9]);
            String countyName = fields.length > 8 ? fields[8].trim() : "";

            if (avgWklyWage != null) {
              Map<String, Object> wageData = new HashMap<>();
              wageData.put("county_fips", areaFips);
              wageData.put("county_name", countyName);
              wageData.put("state_fips", stateFips);
              wageData.put("state_name", stateName);
              wageData.put("average_weekly_wage", avgWklyWage);
              wageData.put("total_employment", avgEmplvl);
              wageData.put("year", year);

              countyWages.put(areaFips, wageData);
            }
          }
          zis.closeEntry();
        }
      }
    }

    if (countyWages.isEmpty()) {
      LOGGER.warn("No county wage data found in QCEW CSV for year {}", year);
      return null;
    }

    LOGGER.info("Extracted wage data for {} counties from QCEW CSV (year {})", countyWages.size(), year);

    // Convert to JSON array format
    try {
      return MAPPER.writeValueAsString(new ArrayList<>(countyWages.values()));
    } catch (Exception e) {
      LOGGER.error("Failed to serialize county wages to JSON: {}", e.getMessage());
      return null;
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
    if (cacheManifest.isCached(dataType, 0, cacheParams)) {
      String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, ftpPath);
      if (cacheStorageProvider.exists(fullPath)) {
        long size = 0L;
        try { size = cacheStorageProvider.getMetadata(fullPath).getSize(); } catch (Exception ignore) {}
        if (size > 0) {
          LOGGER.info("Using cached JOLTS FTP file: {} (from manifest, size={} bytes)", ftpPath, size);
          try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(fullPath)) {
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
    ((CacheManifest) cacheManifest).markCached(dataType, 0, cacheParams, ftpPath, data.length, refreshAfter, "monthly_refresh");
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
      if (data == null) {
        LOGGER.warn("Failed to download {}", dataFile);
        continue;
      }

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
      if (data == null) {
        LOGGER.warn("Failed to download {}", dataFile);
        continue;
      }

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
   * Parses along from a string, returning null if empty or invalid.
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
   * Converts BLS period code to month.
   */
  private int periodToMonth(String period) {
    if (period.startsWith("M")) {
      return Integer.parseInt(period.substring(1));
    } else if (period.startsWith("Q")) {
      // Convert quarterly periods to the first month of quarter
      // Q01 -> January (1), Q02 -> April (4), Q03 -> July (7), Q04 -> October (10)
      int quarter = Integer.parseInt(period.substring(1));
      return (quarter - 1) * 3 + 1;
    }
    return 1; // Default to January for annual or other data (avoid invalid month 0)
  }

  /**
   * Gets a human-readable name for series ID.
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

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map.Entry<String, java.util.List<java.util.Map<String, Object>>> entry : seriesData.entrySet()) {
      for (java.util.Map<String, Object> dataPoint : entry.getValue()) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
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
        dataRecords.add(record);
      }
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("employment_statistics");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "employment_statistics", "employment_statistics");
  }

  /**
   * Writes inflation metrics data to Parquet.
   */
  private void writeInflationMetricsParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map.Entry<String, java.util.List<java.util.Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (java.util.Map<String, Object> dataPoint : entry.getValue()) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
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
        dataRecords.add(record);
      }
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("inflation_metrics");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "inflation_metrics", "inflation_metrics");
  }

  /**
   * Writes wage growth data to Parquet.
   */
  private void writeWageGrowthParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map.Entry<String, java.util.List<java.util.Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (java.util.Map<String, Object> dataPoint : entry.getValue()) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
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

        dataRecords.add(record);
      }
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("wage_growth");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "wage_growth", "wage_growth");
  }

  /**
   * Writes regional employment data to Parquet.
   */
  private void writeRegionalEmploymentParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map.Entry<String, java.util.List<java.util.Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      String stateCode = getStateCode(seriesId);

      for (java.util.Map<String, Object> dataPoint : entry.getValue()) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("date", dataPoint.get("date"));
        record.put("area_code", stateCode);
        record.put("area_name", getStateName(stateCode));
        record.put("area_type", "state");
        record.put("state_code", stateCode);
        record.put("unemployment_rate", dataPoint.get("value"));
        dataRecords.add(record);
      }
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("regional_employment");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "regional_employment", "regional_employment");
  }

  private void writeRegionalCpiParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map.Entry<String, java.util.List<java.util.Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (java.util.Map<String, Object> dataPoint : entry.getValue()) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("area_code", extractAreaCodeFromSeries(seriesId));
        record.put("area_name", extractAreaNameFromSeries(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        dataRecords.add(record);
      }
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("regional_cpi");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "regional_cpi", "regional_cpi");
  }

  private void writeMetroCpiParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map.Entry<String, java.util.List<java.util.Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (java.util.Map<String, Object> dataPoint : entry.getValue()) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("area_code", extractMetroCodeFromSeries(seriesId));
        record.put("area_name", extractMetroNameFromSeries(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        dataRecords.add(record);
      }
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("metro_cpi");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "metro_cpi", "metro_cpi");
  }

  private void writeStateIndustryParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map.Entry<String, java.util.List<java.util.Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (java.util.Map<String, Object> dataPoint : entry.getValue()) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("state_code", extractStateCodeFromSeries(seriesId));
        record.put("state_name", extractStateNameFromSeries(seriesId));
        record.put("industry_code", extractIndustryCodeFromSeries(seriesId));
        record.put("industry_name", extractIndustryNameFromSeries(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        dataRecords.add(record);
      }
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("state_industry");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "state_industry", "state_industry");
  }

  private void writeStateWagesQcewParquet(List<Map<String, Object>> records, String targetPath) throws IOException {
    // Build data records (flat array format)
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map<String, Object> record : records) {
      java.util.Map<String, Object> dataRecord = new java.util.HashMap<>();
      dataRecord.put("year", record.get("year"));
      dataRecord.put("state_fips", record.get("state_fips"));
      dataRecord.put("state_name", record.get("state_name"));
      dataRecord.put("average_weekly_wage", record.get("average_weekly_wage"));
      dataRecord.put("total_employment", record.get("total_employment"));
      dataRecords.add(dataRecord);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("state_wages");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "state_wages", "state_wages");
  }

  private void writeMetroIndustryParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map.Entry<String, java.util.List<java.util.Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (java.util.Map<String, Object> dataPoint : entry.getValue()) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("metro_code", extractMetroCodeFromSeries(seriesId));
        record.put("metro_name", extractMetroNameFromSeries(seriesId));
        record.put("industry_code", extractIndustryCodeFromSeries(seriesId));
        record.put("industry_name", extractIndustryNameFromSeries(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        dataRecords.add(record);
      }
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("metro_industry");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "metro_industry", "metro_industry");
  }

  private void writeMetroWagesQcewParquet(List<Map<String, Object>> records, String targetPath) throws IOException {
    // Build data records (flat array format)
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map<String, Object> record : records) {
      java.util.Map<String, Object> dataRecord = new java.util.HashMap<>();
      dataRecord.put("year", record.get("year"));
      dataRecord.put("metro_area_code", record.get("metro_area_code"));
      dataRecord.put("metro_area_name", record.get("metro_area_name"));
      dataRecord.put("average_weekly_wage", record.get("average_weekly_wage"));
      dataRecord.put("total_employment", record.get("total_employment"));
      dataRecords.add(dataRecord);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("metro_wages");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "metro_wages", "metro_wages");
  }

  private void writeJoltsRegionalParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map.Entry<String, java.util.List<java.util.Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (java.util.Map<String, Object> dataPoint : entry.getValue()) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("region_code", extractRegionCodeFromJoltsSeries(seriesId));
        record.put("region_name", extractRegionNameFromJoltsSeries(seriesId));
        record.put("metric_type", extractJoltsMetricType(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        dataRecords.add(record);
      }
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("jolts_regional");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "jolts_regional", "jolts_regional");
  }

  private void writeCountyWagesParquet(List<Map<String, Object>> countyWages, String targetPath) throws IOException {
    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map<String, Object> countyWage : countyWages) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("county_fips", countyWage.get("county_fips"));
      record.put("county_name", countyWage.get("county_name"));
      record.put("state_fips", countyWage.get("state_fips"));
      record.put("state_name", countyWage.get("state_name"));
      record.put("average_weekly_wage", countyWage.get("average_weekly_wage"));
      record.put("total_employment", countyWage.get("total_employment"));
      // year comes from a partition key (year=*/), not parquet file
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("county_wages");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "county_wages", "county_wages");
  }

  private void writeJoltsStateParquet(List<Map<String, Object>> joltsState, String targetPath) throws IOException {
    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map<String, Object> jolts : joltsState) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("state_fips", jolts.get("state_fips"));
      record.put("state_name", jolts.get("state_name"));
      // year comes from a partition key (year=*/), not parquet file
      record.put("hires_rate", jolts.get("hires_rate"));
      record.put("jobopenings_rate", jolts.get("jobopenings_rate"));
      record.put("quits_rate", jolts.get("quits_rate"));
      record.put("layoffsdischarges_rate", jolts.get("layoffsdischarges_rate"));
      record.put("totalseparations_rate", jolts.get("totalseparations_rate"));
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("jolts_state");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "jolts_state", "jolts_state");
  }

  private void writeJoltsIndustriesParquet(List<Map<String, Object>> industries, String targetPath) throws IOException {
    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map<String, Object> industry : industries) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("industry_code", industry.get("industry_code"));
      record.put("industry_name", industry.get("industry_name"));
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("reference_jolts_industries");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "jolts_industries", "jolts_industries");
  }

  private void writeJoltsDataelementsParquet(List<Map<String, Object>> dataelements, String targetPath) throws IOException {
    // Build data records
    java.util.List<java.util.Map<String, Object>> dataRecords = new java.util.ArrayList<>();
    for (java.util.Map<String, Object> dataelement : dataelements) {
      java.util.Map<String, Object> record = new java.util.HashMap<>();
      record.put("dataelement_code", dataelement.get("dataelement_code"));
      record.put("dataelement_text", dataelement.get("dataelement_text"));
      dataRecords.add(record);
    }

    // Load column metadata and write parquet
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("reference_jolts_dataelements");
    storageProvider.writeAvroParquet(targetPath, columns, dataRecords, "jolts_dataelements", "jolts_dataelements");
  }

  private String extractAreaCodeFromSeries(String seriesId) {
    // Regional CPI series format: CUUR{area}{item}SA0
    if (seriesId.length() >= 8) {
      return seriesId.substring(4, 7);  // Extract region code
    }
    return "UNKNOWN";
  }

  private String extractAreaNameFromSeries(String seriesId) {
    String areaCode = extractAreaCodeFromSeries(seriesId);
    switch (areaCode) {
      case "S10": return "Northeast";
      case "S20": return "Midwest";
      case "S30": return "South";
      case "S40": return "West";
      default: return "Unknown Region";
    }
  }

  private String extractMetroCodeFromSeries(String seriesId) {
    // Metro CPI series format: CUUR{metro_code}{item}SA0
    if (seriesId.length() >= 8) {
      return seriesId.substring(4, 7);
    }
    return "UNKNOWN";
  }

  private String extractMetroNameFromSeries(String seriesId) {
    // Simplified metro name extraction - in production, use a lookup map
    String code = extractMetroCodeFromSeries(seriesId);
    return "Metro Area " + code;
  }

  private String extractStateCodeFromSeries(String seriesId) {
    // State employment series format: SMS{state_fips}{supersector}...
    if (seriesId.startsWith("SMS") && seriesId.length() >= 5) {
      return seriesId.substring(3, 5);  // Extract state FIPS
    }
    // State wages series format: ENU{state_fips}...
    if (seriesId.startsWith("ENU") && seriesId.length() >= 5) {
      return seriesId.substring(3, 5);
    }
    return "UNKNOWN";
  }

  private String extractStateNameFromSeries(String seriesId) {
    // Simplified state name extraction
    String fips = extractStateCodeFromSeries(seriesId);
    return "State " + fips;
  }

  private String extractIndustryCodeFromSeries(String seriesId) {
    // State industry: SMS{state}{supersector}0000000000SA
    if (seriesId.startsWith("SMS") && seriesId.length() >= 10) {
      return seriesId.substring(5, 13);  // Extract supersector + detail codes
    }
    // Metro industry: SMU{metro}{supersector}0000000001SA
    if (seriesId.startsWith("SMU") && seriesId.length() >= 12) {
      return seriesId.substring(8, 16);
    }
    return "UNKNOWN";
  }

  private String extractIndustryNameFromSeries(String seriesId) {
    // Simplified industry name extraction
    String code = extractIndustryCodeFromSeries(seriesId);
    return "Industry " + code;
  }

  private String extractRegionCodeFromJoltsSeries(String seriesId) {
    // JOLTS series format: JTU{region}00...
    if (seriesId.startsWith("JTU") && seriesId.length() >= 6) {
      return seriesId.substring(3, 6);
    }
    return "UNKNOWN";
  }

  private String extractRegionNameFromJoltsSeries(String seriesId) {
    String code = extractRegionCodeFromJoltsSeries(seriesId);
    switch (code) {
      case "100": return "Northeast";
      case "200": return "Midwest";
      case "300": return "South";
      case "400": return "West";
      default: return "Unknown Region";
    }
  }

  private String extractJoltsMetricType(String seriesId) {
    // JOLTS series format: JTU{region}{industry}{datatype}...
    if (seriesId.length() >= 12) {
      String datatype = seriesId.substring(9, 11);
      switch (datatype) {
        case "HI": return "hires";
        case "TS": return "separations";
        case "JO": return "openings";
        case "QU": return "quits";
        case "LD": return "layoffs";
        default: return "unknown";
      }
    }
    return "unknown";
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

    // Extract data type from the filename
    String fileName = targetFilePath.substring(targetFilePath.lastIndexOf('/') + 1);

    // Read employment statistics JSON files and convert to employment_statistics.parquet
    switch (fileName) {
    case "employment_statistics.parquet":
      convertEmploymentStatisticsToParquet(sourceDirPath, targetFilePath);
      break;
    case "inflation_metrics.parquet":
      convertInflationMetricsToParquet(sourceDirPath, targetFilePath);
      break;
    case "wage_growth.parquet":
      convertWageGrowthToParquet(sourceDirPath, targetFilePath);
      break;
    case "regional_employment.parquet":
      convertRegionalEmploymentToParquet(sourceDirPath, targetFilePath);
      break;
    case "regional_cpi.parquet":
      convertRegionalCpiToParquet(sourceDirPath, targetFilePath);
      break;
    case "metro_cpi.parquet":
      convertMetroCpiToParquet(sourceDirPath, targetFilePath);
      break;
    case "state_industry.parquet":
      convertStateIndustryToParquet(sourceDirPath, targetFilePath);
      break;
    case "state_wages.parquet":
      convertStateWagesToParquet(sourceDirPath, targetFilePath);
      break;
    case "metro_industry.parquet":
      convertMetroIndustryToParquet(sourceDirPath, targetFilePath);
      break;
    case "metro_wages.parquet":
      convertMetroWagesToParquet(sourceDirPath, targetFilePath);
      break;
    case "jolts_regional.parquet":
      convertJoltsRegionalToParquet(sourceDirPath, targetFilePath);
      break;
    case "county_wages.parquet":
      convertCountyWagesToParquet(sourceDirPath, targetFilePath);
      break;
    case "jolts_state.parquet":
      convertJoltsStateToParquet(sourceDirPath, targetFilePath);
      break;
    case "jolts_industries.parquet":
      convertJoltsIndustriesToParquet(sourceDirPath, targetFilePath);
      break;
    case "jolts_dataelements.parquet":
      convertJoltsDataelementsToParquet(sourceDirPath, targetFilePath);
      break;
    }
  }

  private void convertEmploymentStatisticsToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read employment statistics JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "employment_statistics.json");
    if (cacheStorageProvider.exists(jsonFilePath)) {
      try {
        long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
        LOGGER.debug("[DEBUG] employment_statistics JSON exists, size={} bytes: {}", size, jsonFilePath);
      } catch (Exception metaEx) {
        LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
      }
      try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
           java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
        bin.mark(2 * 1024 * 1024);
        byte[] head = bin.readNBytes(16 * 1024);
        LOGGER.debug("[DEBUG] Head of employment_statistics JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
      }
      try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
           java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(reader);
        LOGGER.debug("[DEBUG] employment_statistics JSON root type: {}", root.getNodeType());

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
      try {
        long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
        LOGGER.debug("[DEBUG] inflation_metrics JSON exists, size={} bytes: {}", size, jsonFilePath);
      } catch (Exception metaEx) {
        LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
      }
      try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
           java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
        bin.mark(2 * 1024 * 1024);
        byte[] head = bin.readNBytes(16 * 1024);
        LOGGER.debug("[DEBUG] Head of inflation_metrics JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
      }
      try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
           java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(reader);
        LOGGER.debug("[DEBUG] inflation_metrics JSON root type: {}", root.getNodeType());

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
      try {
        long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
        LOGGER.debug("[DEBUG] wage_growth JSON exists, size={} bytes: {}", size, jsonFilePath);
      } catch (Exception metaEx) {
        LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
      }
      try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
           java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
        bin.mark(2 * 1024 * 1024);
        byte[] head = bin.readNBytes(16 * 1024);
        LOGGER.debug("[DEBUG] Head of wage_growth JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
      }
      try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
           java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(reader);
        LOGGER.debug("[DEBUG] wage_growth JSON root type: {}", root.getNodeType());

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
                // BLS API provides calculations.pct_changes.12 for a 12-month change
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

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] regional_employment JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of regional_employment JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
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

  private void convertRegionalCpiToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read regional CPI JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "regional_cpi.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No regional_cpi.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] regional_cpi JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of regional_cpi JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
    }

    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);
      LOGGER.debug("[DEBUG] regional_cpi JSON root type: {}", root.getNodeType());

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
      LOGGER.error("Error reading BLS regional CPI JSON file {}: {}", jsonFilePath, e.getMessage(), e);
    }

    if (seriesData.isEmpty()) {
      LOGGER.warn("No data found in regional CPI JSON file");
      return;
    }

    // Write to parquet using regional CPI schema
    LOGGER.info("Converting regional CPI data with {} series to parquet: {}", seriesData.size(), targetPath);
    writeRegionalCpiParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS regional CPI data to parquet: {}", targetPath);
  }

  private void convertMetroCpiToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read metro CPI JSON file from cache using cacheStorageProvider
    targetPath = targetPath.replace("/type=metro_cpi", "/type=metro_cpi/frequency=monthly");
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "metro_cpi.json").replace("/type=metro_cpi", "/type=metro_cpi/frequency=monthly");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No metro_cpi.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] metro_cpi JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of metro_cpi JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
      bin.reset();
      try (java.io.InputStreamReader readerHead = new java.io.InputStreamReader(bin, StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(readerHead);
        LOGGER.debug("[DEBUG] metro_cpi JSON root type: {}", root.getNodeType());
      }
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
      LOGGER.error("Error reading BLS metro CPI JSON file {}: {}", jsonFilePath, e.getMessage(), e);
    }

    if (seriesData.isEmpty()) {
      LOGGER.warn("No data found in metro CPI JSON file");
      return;
    }

    // Write to parquet using metro CPI schema
    LOGGER.info("Converting metro CPI data with {} series to parquet: {}", seriesData.size(), targetPath);
    writeMetroCpiParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS metro CPI data to parquet: {}", targetPath);
  }

  private void convertStateIndustryToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read state industry JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "state_industry.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No state_industry.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] state_industry JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of state_industry JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
    }

    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);
      LOGGER.debug("[DEBUG] state_industry JSON root type: {}", root.getNodeType());

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
      LOGGER.error("Error reading BLS state industry JSON file {}: {}", jsonFilePath, e.getMessage(), e);
    }

    if (seriesData.isEmpty()) {
      LOGGER.warn("No data found in state industry JSON file");
      return;
    }

    // Write to parquet using state industry schema
    LOGGER.info("Converting state industry data with {} series to parquet: {}", seriesData.size(), targetPath);
    writeStateIndustryParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS state industry data to parquet: {}", targetPath);
  }

  private void convertStateWagesToParquet(String sourceDirPath, String targetPath) throws IOException {
    // Read state wages JSON file from cache using cacheStorageProvider
    // Note: state_wages.json is in flat array format from QCEW CSV, not BLS API format
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "state_wages.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No state_wages.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] state_wages JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of state_wages JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
    }

    List<Map<String, Object>> records = new ArrayList<>();
    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);

      // Check if the root is an array (QCEW format)
      if (root.isArray()) {
        for (JsonNode record : root) {
          Map<String, Object> dataPoint = new HashMap<>();
          dataPoint.put("year", record.get("year").asInt());
          dataPoint.put("state_fips", record.get("state_fips").asText());
          dataPoint.put("state_name", record.get("state_name").asText());
          dataPoint.put("average_weekly_wage", record.get("average_weekly_wage").asDouble());
          dataPoint.put("total_employment", record.get("total_employment").asInt());
          records.add(dataPoint);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error reading BLS state wages JSON file {}: {}", jsonFilePath, e.getMessage(), e);
      return;
    }

    if (records.isEmpty()) {
      LOGGER.warn("No data found in state wages JSON file");
      return;
    }

    // Write to parquet using a simplified state wages schema
    LOGGER.info("Converting state wages data with {} records to parquet: {}", records.size(), targetPath);
    writeStateWagesQcewParquet(records, targetPath);
    LOGGER.info("Converted BLS state wages data to parquet: {}", targetPath);
  }

  private void convertMetroIndustryToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read metro industry JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "metro_industry.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No metro_industry.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] metro_industry JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of metro_industry JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
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
      LOGGER.error("Error reading BLS metro industry JSON file {}: {}", jsonFilePath, e.getMessage(), e);
    }

    if (seriesData.isEmpty()) {
      LOGGER.warn("No data found in metro industry JSON file");
      return;
    }

    // Write to parquet using metro industry schema
    LOGGER.info("Converting metro industry data with {} series to parquet: {}", seriesData.size(), targetPath);
    writeMetroIndustryParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS metro industry data to parquet: {}", targetPath);
  }

  private void convertMetroWagesToParquet(String sourceDirPath, String targetPath) throws IOException {
    // Read metro wages JSON file from cache using cacheStorageProvider
    // Note: metro_wages.json is in flat array format from QCEW CSV, not BLS API format
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "metro_wages.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No metro_wages.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] metro_wages JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of metro_wages JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
    }

    List<Map<String, Object>> records = new ArrayList<>();
    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);

      // Check if the root is an array (QCEW format)
      if (root.isArray()) {
        for (JsonNode record : root) {
          Map<String, Object> dataPoint = new HashMap<>();
          dataPoint.put("year", record.get("year").asInt());
          dataPoint.put("metro_area_code", record.get("metro_area_code").asText());
          dataPoint.put("metro_area_name", record.get("metro_area_name").asText());
          dataPoint.put("average_weekly_wage", record.get("average_weekly_wage").asDouble());
          if (record.has("total_employment") && !record.get("total_employment").isNull()) {
            dataPoint.put("total_employment", record.get("total_employment").asInt());
          } else {
            dataPoint.put("total_employment", null);
          }
          records.add(dataPoint);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error reading BLS metro wages JSON file {}: {}", jsonFilePath, e.getMessage(), e);
      return;
    }

    if (records.isEmpty()) {
      LOGGER.warn("No data found in metro wages JSON file");
      return;
    }

    // Write to parquet using a simplified metro wages schema
    LOGGER.info("Converting metro wages data with {} records to parquet: {}", records.size(), targetPath);
    writeMetroWagesQcewParquet(records, targetPath);
    LOGGER.info("Converted BLS metro wages data to parquet: {}", targetPath);
  }

  private void convertJoltsRegionalToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read JOLTS regional JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "jolts_regional.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No jolts_regional.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] JOLTS regional JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }

    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStream)) {
      // Small preview for debugging without consuming the stream
      bin.mark(2 * 1024 * 1024); // allow reset within 2 MB
      byte[] head = bin.readNBytes(64 * 1024); // read up to 64 KB for preview
      LOGGER.debug("[DEBUG] Head of JOLTS regional JSON ({} bytes):\n{}",
          head.length, new String(head, StandardCharsets.UTF_8));
      bin.reset();

      try (java.io.InputStreamReader reader = new java.io.InputStreamReader(bin, StandardCharsets.UTF_8)) {
        JsonNode root = MAPPER.readTree(reader);
        LOGGER.debug("[DEBUG] JOLTS regional JSON root type: {}", root.getNodeType());

        if (root.isArray()) {
          // Handle our FTP-derived annual array format
          for (JsonNode node : root) {
            String regionAlpha = node.path("region_code").asText("");
            if (regionAlpha.isEmpty()) {
              String regionName = node.path("region").asText("");
              // fallback mapping by name
              if ("Northeast".equalsIgnoreCase(regionName)) regionAlpha = "NE";
              else if ("Midwest".equalsIgnoreCase(regionName)) regionAlpha = "MW";
              else if ("South".equalsIgnoreCase(regionName)) regionAlpha = "SO";
              else if ("West".equalsIgnoreCase(regionName)) regionAlpha = "WE";
            }

            // Map alpha code to numeric code expected by extractors
            String regionNumeric;
            switch (regionAlpha) {
              case "NE": regionNumeric = "100"; break;
              case "MW": regionNumeric = "200"; break;
              case "SO": regionNumeric = "300"; break;
              case "WE": regionNumeric = "400"; break;
              default: regionNumeric = "999"; // Unknown
            }

            int year = node.path("year").asInt();
            String date = String.format("%04d-01-01", year); // annual aggregate as Jan 1st

            // Pairs of (jsonField, datatypeCode)
            String[][] metrics = new String[][]{
                {"jobopenings_rate", "JO"},
                {"hires_rate", "HI"},
                {"totalseparations_rate", "TS"},
                {"quits_rate", "QU"},
                {"layoffsdischarges_rate", "LD"}
            };

            for (String[] metric : metrics) {
              String field = metric[0];
              String dtype = metric[1];
              if (node.has(field) && node.get(field).isNumber()) {
                double value = node.get(field).asDouble();
                // Synthesize a seriesId compatible with existing extractors
                // Format: "JTU" + region(3) + "000" + datatype(2) + "0000" (length >= 12)
                String seriesId = "JTU" + regionNumeric + "000" + dtype + "0000";

                List<Map<String, Object>> dataPoints = seriesData.computeIfAbsent(seriesId, k -> new ArrayList<>());
                Map<String, Object> dataPoint = new HashMap<>();
                dataPoint.put("date", date);
                dataPoint.put("value", value);
                dataPoints.add(dataPoint);
              }
            }
          }
        } else if (root.has("Results") && root.get("Results").has("series")) {
          // Handle BLS API-like structure (not typical for FTP regional file, but supported for robustness)
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
        } else {
          LOGGER.warn("Unexpected JSON shape for jolts_regional: root={} at {}", root.getNodeType(), jsonFilePath);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error reading BLS JOLTS regional JSON file {}: {}", jsonFilePath, e.getMessage(), e);
    }

    if (seriesData.isEmpty()) {
      LOGGER.warn("No data found in JOLTS regional JSON file");
      return;
    }

    // Write to parquet using JOLTS regional schema
    LOGGER.info("Converting JOLTS regional data with {} series to parquet: {}", seriesData.size(), targetPath);
    writeJoltsRegionalParquet(seriesData, targetPath);
    LOGGER.info("Converted BLS JOLTS regional data to parquet: {}", targetPath);
  }

  private void convertCountyWagesToParquet(String sourceDirPath, String targetPath) throws IOException {
    // Read county wages JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "county_wages.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No county_wages.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] county_wages JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of county_wages JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
    }

    List<Map<String, Object>> countyWages;
    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      // JSON is a simple array of county wage objects
      countyWages = MAPPER.readValue(reader, new TypeReference<List<Map<String, Object>>>() {});
    } catch (Exception e) {
      LOGGER.error("Error reading county wages JSON file {}: {}", jsonFilePath, e.getMessage(), e);
      return;
    }

    if (countyWages.isEmpty()) {
      LOGGER.warn("No data found in county wages JSON file");
      return;
    }

    // Write to parquet
    LOGGER.info("Converting county wages data with {} records to parquet: {}", countyWages.size(), targetPath);
    writeCountyWagesParquet(countyWages, targetPath);
    LOGGER.info("Converted county wages data to parquet: {}", targetPath);
  }

  private void convertJoltsStateToParquet(String sourceDirPath, String targetPath) throws IOException {
    // Read JOLTS state JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "jolts_state.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No jolts_state.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] jolts_state JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of jolts_state JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
    }

    List<Map<String, Object>> joltsState;
    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      // JSON is a simple array of state JOLTS objects
      joltsState = MAPPER.readValue(reader, new TypeReference<List<Map<String, Object>>>() {});
    } catch (Exception e) {
      LOGGER.error("Error reading JOLTS state JSON file {}: {}", jsonFilePath, e.getMessage(), e);
      return;
    }

    if (joltsState.isEmpty()) {
      LOGGER.warn("No data found in JOLTS state JSON file");
      return;
    }

    // Write to parquet
    LOGGER.info("Converting JOLTS state data with {} records to parquet: {}", joltsState.size(), targetPath);
    writeJoltsStateParquet(joltsState, targetPath);
    LOGGER.info("Converted JOLTS state data to parquet: {}", targetPath);
  }

  private void convertJoltsIndustriesToParquet(String sourceDirPath, String targetPath) throws IOException {
    // Read JOLTS industries reference JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "jolts_industries.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No jolts_industries.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] jolts_industries JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of jolts_industries JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
    }

    List<Map<String, Object>> industries;
    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      // JSON is a simple array of industry reference objects
      industries = MAPPER.readValue(reader, new TypeReference<List<Map<String, Object>>>() {});
    } catch (Exception e) {
      LOGGER.error("Error reading JOLTS industries JSON file {}: {}", jsonFilePath, e.getMessage(), e);
      return;
    }

    if (industries.isEmpty()) {
      LOGGER.warn("No data found in JOLTS industries JSON file");
      return;
    }

    // Write to parquet
    LOGGER.info("Converting JOLTS industries reference with {} records to parquet: {}", industries.size(), targetPath);
    writeJoltsIndustriesParquet(industries, targetPath);
    LOGGER.info("Converted JOLTS industries reference to parquet: {}", targetPath);
  }

  private void convertJoltsDataelementsToParquet(String sourceDirPath, String targetPath) throws IOException {
    // Read JOLTS data elements reference JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "jolts_dataelements.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No jolts_dataelements.json found in {}", sourceDirPath);
      return;
    }

    try {
      long size = cacheStorageProvider.getMetadata(jsonFilePath).getSize();
      LOGGER.debug("[DEBUG] jolts_dataelements JSON exists, size={} bytes: {}", size, jsonFilePath);
    } catch (Exception metaEx) {
      LOGGER.debug("[DEBUG] Unable to read metadata for {}: {}", jsonFilePath, metaEx.getMessage());
    }
    try (java.io.InputStream inputStreamHead = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.BufferedInputStream bin = new java.io.BufferedInputStream(inputStreamHead)) {
      bin.mark(2 * 1024 * 1024);
      byte[] head = bin.readNBytes(16 * 1024);
      LOGGER.debug("[DEBUG] Head of jolts_dataelements JSON ({} bytes):\n{}", head.length, new String(head, StandardCharsets.UTF_8));
    }

    List<Map<String, Object>> dataelements;
    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      // JSON is a simple array of data element reference objects
      dataelements = MAPPER.readValue(reader, new TypeReference<List<Map<String, Object>>>() {});
    } catch (Exception e) {
      LOGGER.error("Error reading JOLTS data elements JSON file {}: {}", jsonFilePath, e.getMessage(), e);
      return;
    }

    if (dataelements.isEmpty()) {
      LOGGER.warn("No data found in JOLTS data elements JSON file");
      return;
    }

    // Write to parquet
    LOGGER.info("Converting JOLTS data elements reference with {} records to parquet: {}", dataelements.size(), targetPath);
    writeJoltsDataelementsParquet(dataelements, targetPath);
    LOGGER.info("Converted JOLTS data elements reference to parquet: {}", targetPath);
  }

  // ===== Metadata-Driven Employment Statistics Methods =====

  /**
   * Downloads employment statistics data using metadata-driven pattern.
   * This is a pilot implementation for testing BLS POST infrastructure.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @throws IOException if download fails
   * @throws InterruptedException if download is interrupted
   */
  public void downloadEmploymentStatisticsMetadata(int startYear, int endYear)
      throws IOException, InterruptedException {
    String tableName = "employment_statistics";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    int downloadedCount = 0;
    int skippedCount = 0;

    for (int year = startYear; year <= endYear; year++) {
      // Build variables map
      Map<String, String> variables = new HashMap<>();
      variables.put("year", String.valueOf(year));
      variables.put("frequency", "monthly");

      // Resolve path using pattern
      String relativePath = resolveJsonPath(pattern, variables);

      // Check if already cached
      Map<String, String> params = new HashMap<>();
      params.put("frequency", "monthly");

      if (isCachedOrExists(tableName, year, params)) {
        skippedCount++;
        continue;
      }

      // Download via metadata-driven executeDownload()
      try {
        String cachedPath = executeDownload(tableName, variables);

        // Mark as downloaded in cache manifest
        try {
          String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, relativePath);
          long fileSize = cacheStorageProvider.getMetadata(fullPath).getSize();
          cacheManifest.markCached(tableName, year, params, relativePath, fileSize);
        } catch (Exception ex) {
          LOGGER.warn("Failed to mark {} as cached in manifest: {}", relativePath, ex.getMessage());
        }

        downloadedCount++;
      } catch (Exception e) {
        LOGGER.error("Error downloading employment statistics for year {}: {}", year, e.getMessage());
      }
    }

    // Save manifest after all downloads complete
    try {
      cacheManifest.save(operatingDirectory);
    } catch (Exception e) {
      LOGGER.error("Failed to save cache manifest: {}", e.getMessage());
    }

    LOGGER.info("Employment statistics download complete: downloaded {} years, skipped {} (cached)",
        downloadedCount, skippedCount);
  }

  /**
   * Converts employment statistics data using metadata-driven pattern.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   * @throws IOException if conversion fails
   */
  public void convertEmploymentStatisticsMetadata(int startYear, int endYear)
      throws IOException {
    String tableName = "employment_statistics";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    int convertedCount = 0;
    int skippedCount = 0;

    for (int year = startYear; year <= endYear; year++) {
      // Build paths for this year
      Map<String, String> variables = new HashMap<>();
      variables.put("year", String.valueOf(year));
      variables.put("frequency", "monthly");

      String rawPath = resolveJsonPath(pattern, variables);
      String parquetPath = resolveParquetPath(pattern, variables);

      // Check if conversion needed
      Map<String, String> params = new HashMap<>();
      params.put("frequency", "monthly");

      if (isParquetConvertedOrExists(tableName, year, params, rawPath, parquetPath)) {
        skippedCount++;
        continue;
      }

      // Convert via metadata-driven approach
      try {
        convertCachedJsonToParquet(tableName, variables);
        cacheManifest.markParquetConverted(tableName, year, params, parquetPath);
        convertedCount++;
      } catch (Exception e) {
        LOGGER.error("Error converting employment statistics for year {}: {}", year, e.getMessage());
      }
    }

    LOGGER.info("Employment statistics conversion complete: converted {} years, skipped {} (up-to-date)",
        convertedCount, skippedCount);
  }

  /**
   * Downloads BLS reference tables (JOLTS industries and dataelements).
   * Uses year=0 sentinel value for reference tables without year dimension.
   */
  @Override public void downloadReferenceData() throws IOException {
    LOGGER.info("Downloading BLS JOLTS reference tables");

    // Download and convert reference_jolts_industries
    String joltsIndustriesParquetPath =
        storageProvider.resolvePath(parquetDirectory, "type=reference/jolts_industries.parquet");
    String joltsIndustriesRawPath =
        cacheStorageProvider.resolvePath(cacheDirectory, "type=reference/jolts_industries.json");

    if (!isParquetConvertedOrExists("jolts_industries", 0, new java.util.HashMap<>(),
        joltsIndustriesRawPath, joltsIndustriesParquetPath)) {
      java.util.Map<String, String> variables = new java.util.HashMap<>();
      convertCachedJsonToParquet("jolts_industries", variables);
      cacheManifest.markParquetConverted("jolts_industries", 0, null,
          joltsIndustriesParquetPath);
      LOGGER.info("Converted reference_jolts_industries to parquet");
    } else {
      LOGGER.info("reference_jolts_industries already converted, skipping");
    }

    // Download and convert reference_jolts_dataelements
    String joltsDataelementsParquetPath =
        storageProvider.resolvePath(parquetDirectory, "type=reference/jolts_dataelements.parquet");
    String joltsDataelementsRawPath =
        cacheStorageProvider.resolvePath(cacheDirectory, "type=reference/jolts_dataelements.json");

    if (!isParquetConvertedOrExists("jolts_dataelements", 0, new java.util.HashMap<>(),
        joltsDataelementsRawPath, joltsDataelementsParquetPath)) {
      java.util.Map<String, String> variables = new java.util.HashMap<>();
      convertCachedJsonToParquet("jolts_dataelements", variables);
      cacheManifest.markParquetConverted("jolts_dataelements", 0, null,
          joltsDataelementsParquetPath);
      LOGGER.info("Converted reference_jolts_dataelements to parquet");
    } else {
      LOGGER.info("reference_jolts_dataelements already converted, skipping");
    }

    LOGGER.info("Completed BLS JOLTS reference tables download");
  }
}
