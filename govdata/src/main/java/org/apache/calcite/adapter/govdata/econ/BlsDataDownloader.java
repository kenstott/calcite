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

import com.fasterxml.jackson.core.type.TypeReference;
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
     * @param metroAreaCode Metro area code (e.g., "A100" for NYC)
     * Area code "A" indicates semi-annual publication frequency
     */
    public static String getMetroCpiSeriesId(String metroAreaCode) {
      return "CUUR" + metroAreaCode + "SA0";
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
     * Gets NAICS supersector name from code.
     */
    public static String getNaicsSupersectorName(String supersectorCode) {
      return NAICS_SUPERSECTORS.getOrDefault(supersectorCode, "Unknown Sector");
    }

    /**
     * Generates BLS state wage series ID from QCEW (Quarterly Census of Employment and Wages).
     * Format: ENU{AREA}{DATATYPE}{SIZE}{OWNERSHIP}{INDUSTRY}
     * Total: 17 chars (EN=2, U=1, Area=5, DataType=1, Size=1, Ownership=1, Industry=6)
     * Data types: 1 = Employment, 3 = Average weekly wages
     *
     * @param stateFips 2-digit state FIPS code
     * @param dataType "1" for employment, "3" for wages
     * @return QCEW series ID
     */
    public static String getStateWageSeriesId(String stateFips, String dataType) {
      // Area: stateFips + "000" (statewide), DataType: 3 (wages), Size: 0 (all),
      // Ownership: 5 (private), Industry: 101010 (total all industries)
      return "ENU" + stateFips + "000" + dataType + "05101010";
    }

    /**
     * Gets all state average weekly wage series IDs (51 jurisdictions).
     * Uses data type "3" for average weekly wages.
     */
    public static List<String> getAllStateWageSeriesIds() {
      List<String> seriesIds = new ArrayList<>();
      for (String stateFips : STATE_FIPS_MAP.values()) {
        seriesIds.add(getStateWageSeriesId(stateFips, "3"));
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

    /**
     * Generates BLS metro area wage series ID from QCEW.
     * Format: ENU{AREA}{DATATYPE}{SIZE}{OWNERSHIP}{INDUSTRY}
     * Total: 17 chars (EN=2, U=1, Area=5, DataType=1, Size=1, Ownership=1, Industry=6)
     * Example: ENU9356130510101 for NYC wages
     *
     * @param metroCode Metro area code (e.g., "A100" for NYC)
     * @return QCEW metro wage series ID
     */
    public static String getMetroWageSeriesId(String metroCode) {
      String blsAreaCode = METRO_BLS_AREA_CODES.get(metroCode);
      if (blsAreaCode == null) {
        throw new IllegalArgumentException("Unknown metro code: " + metroCode);
      }
      // Use last 5 digits as area code, DataType: 3 (wages), Size: 0 (all),
      // Ownership: 5 (private), Industry: 101010 (total)
      String areaOnly = blsAreaCode.substring(2);  // Remove state code, use area code
      return "ENU" + areaOnly + "305101010";
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
     * Format: JT{S/U}{INDUSTRY}{STATE}{AREA}{SIZE}{DATAELEMENT}{RATELEVEL}
     * Total: 21 characters (JT=2, S/U=1, Industry=6, State=2, Area=5, Size=2, DataElement=2, RateLevel=1)
     * Regional data only available at total nonfarm level
     *
     * @param regionCode Region code (1000=NE, 2000=MW, 3000=S, 4000=W)
     * @param metric Metric code (JOR=Job Openings Rate, HIR=Hires Rate, TSR=Total Separations,
     *               QUR=Quits Rate, LDR=Layoffs/Discharges Rate)
     * @return JOLTS series ID
     */
    public static String getJoltsRegionalSeriesId(String regionCode, String metric) {
      // Format: JTS + Industry(6) + State(2) + Area(5) + Size(2) + DataElement(2) + RateLevel(1)
      // Using total nonfarm (000000), region as state code first 2 digits of regionCode,
      // region code as area, all sizes (00), metric as data element
      String state = regionCode.substring(0, 2);  // "10" from "1000"
      return "JTS000000" + state + regionCode.substring(2) + "00" + metric + "R";
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
  public static final String TABLE_COUNTY_WAGES = "county_wages";
  public static final String TABLE_COUNTY_QCEW = "county_qcew";
  public static final String TABLE_METRO_INDUSTRY = "metro_industry";
  public static final String TABLE_METRO_WAGES = "metro_wages";
  public static final String TABLE_JOLTS_REGIONAL = "jolts_regional";
  public static final String TABLE_JOLTS_STATE = "jolts_state";
  public static final String TABLE_WAGE_GROWTH = "wage_growth";
  public static final String TABLE_REGIONAL_EMPLOYMENT = "regional_employment";

  // Reference table constants
  public static final String TABLE_REFERENCE_JOLTS_INDUSTRIES = "reference_jolts_industries";
  public static final String TABLE_REFERENCE_JOLTS_DATAELEMENTS = "reference_jolts_dataelements";

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
   * @param dataType Type of data being cached
   * @param year Year of data
   * @param cacheParams Additional cache parameters
   * @param relativePath Relative path for cache file
   * @param rawJson Raw JSON response from BLS API
   * @return true if response was saved (data or 404), false if retriable error
   * @throws IOException If JSON parsing fails
   */
  private boolean validateAndSaveBlsResponse(String dataType, int year,
      Map<String, String> cacheParams, String relativePath, String rawJson) throws IOException {

    JsonNode response = MAPPER.readTree(rawJson);
    String status = response.path("status").asText("UNKNOWN");

    if ("REQUEST_SUCCEEDED".equals(status)) {
      // Check if data is actually present
      JsonNode results = response.path("Results");
      JsonNode series = results.path("series");

      if (!series.isMissingNode() && series.isArray() && series.size() > 0) {
        // Has data - save normally
        saveToCache(dataType, year, cacheParams, relativePath, rawJson);
        return true;
      } else {
        // 404/No data - save anyway to create empty parquet file
        LOGGER.info("No data available for {} year {} - saving empty response", dataType, year);
        saveToCache(dataType, year, cacheParams, relativePath, rawJson);
        return true;
      }
    }

    // Error response (rate limit, server error, etc) - don't save
    JsonNode messageNode = response.path("message");
    String message = messageNode.isArray() && messageNode.size() > 0
        ? messageNode.get(0).asText()
        : messageNode.asText("No error message");
    LOGGER.warn("BLS API error for {} year {}: {} - {} (not cached, will retry)",
                dataType, year, status, message);
    return false;
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

      // Check if we can extend current range
      if (year == rangeEnd + 1 && (year - rangeStart) < maxYears) {
        rangeEnd = year;  // Extend range
      } else {
        // Start new range
        ranges.add(new int[]{rangeStart, rangeEnd});
        rangeStart = year;
        rangeEnd = year;
      }
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
   * @param uncachedYears Specific years that need fetching (may be non-contiguous)
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

      // Merge if year already has data from previous batch
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
   * Merges two BLS API JSON responses (for same year, different series batches).
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
          String message = messageNode.isArray() && messageNode.size() > 0
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
   * Downloads employment statistics using default date range from environment.
   */
  public File downloadEmploymentStatistics() throws IOException, InterruptedException {
    return downloadEmploymentStatistics(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads employment statistics data and converts to Parquet.
   */
  public File downloadEmploymentStatistics(int startYear, int endYear) throws IOException, InterruptedException {
    File lastFile = null;

    // Series IDs to fetch (constant across all years)
    List<String> seriesIds =
        List.of(Series.UNEMPLOYMENT_RATE,
        Series.EMPLOYMENT_LEVEL,
        Series.LABOR_FORCE_PARTICIPATION);

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = buildPartitionPath("employment_statistics", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/employment_statistics.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("employment_statistics", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached employment statistics for year {} - skipping", year);
        lastFile = new File(jsonFilePath);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All employment statistics data cached (years {}-{})", startYear, endYear);
      return lastFile;
    }

    // 2. Batch fetch uncached years (optimized: up to 20 contiguous years per API call)
    LOGGER.info("Fetching employment statistics for {} uncached years", uncachedYears.size());
    Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, uncachedYears);

    // 3. Save each uncached year
    for (int year : uncachedYears) {
      String outputDirPath = buildPartitionPath("employment_statistics", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/employment_statistics.json";
      Map<String, String> cacheParams = new HashMap<>();

      String rawJson = resultsByYear.get(year);
      if (rawJson != null && validateAndSaveBlsResponse("employment_statistics", year, cacheParams, jsonFilePath, rawJson)) {
        lastFile = new File(jsonFilePath);
      }
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
    File lastFile = null;

    List<String> seriesIds = Series.getAllRegionalCpiSeriesIds();

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = buildPartitionPath("regional_cpi", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/regional_cpi.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("regional_cpi", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached regional CPI for year {} - skipping", year);
        lastFile = new File(jsonFilePath);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All regional CPI data cached (years {}-{})", startYear, endYear);
      return lastFile;
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
      if (rawJson != null && validateAndSaveBlsResponse("regional_cpi", year, cacheParams, jsonFilePath, rawJson)) {
        lastFile = new File(jsonFilePath);
      }
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
    File lastFile = null;

    List<String> seriesIds = Series.getAllMetroCpiSeriesIds();
    LOGGER.info("Metro CPI series examples: {}, {}, {}",
                seriesIds.size() > 0 ? seriesIds.get(0) : "none",
                seriesIds.size() > 1 ? seriesIds.get(1) : "none",
                seriesIds.size() > 2 ? seriesIds.get(2) : "none");

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = buildPartitionPath("metro_cpi", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/metro_cpi.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("metro_cpi", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached metro CPI for year {} - skipping", year);
        lastFile = new File(jsonFilePath);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All metro CPI data cached (years {}-{})", startYear, endYear);
      return lastFile;
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
      } else if (validateAndSaveBlsResponse("metro_cpi", year, cacheParams, jsonFilePath, rawJson)) {
        lastFile = new File(jsonFilePath);
      }
    }

    return lastFile;
  }

  public File downloadStateIndustryEmployment() throws IOException, InterruptedException {
    return downloadStateIndustryEmployment(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads employment by industry data for all 51 U.S. jurisdictions (50 states + DC)
   * across 22 NAICS supersector codes. Generates 1,122 series (51 × 22).
   *
   * <p>Optimized with year-batching to reduce API calls from ~345 (23 batches × 15 years)
   * to ~46 (23 batches × 2 year-batches).
   */
  public File downloadStateIndustryEmployment(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading state industry employment for {} states × {} sectors ({} series) for {}-{}",
                STATE_FIPS_MAP.size(), NAICS_SUPERSECTORS.size(),
                STATE_FIPS_MAP.size() * NAICS_SUPERSECTORS.size(), startYear, endYear);

    File lastFile = null;
    List<String> seriesIds = Series.getAllStateIndustryEmploymentSeriesIds();
    LOGGER.info("Generated {} state industry employment series IDs", seriesIds.size());
    LOGGER.info("State industry series examples: {}, {}, {}",
                seriesIds.size() > 0 ? seriesIds.get(0) : "none",
                seriesIds.size() > 1 ? seriesIds.get(1) : "none",
                seriesIds.size() > 2 ? seriesIds.get(2) : "none");

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String jsonFilePath = buildPartitionPath("state_industry", DataFrequency.QUARTERLY, year) + "/state_industry.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("state_industry", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached state industry employment for year {} - skipping", year);
        lastFile = new File(jsonFilePath);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All state industry employment data cached (years {}-{})", startYear, endYear);
      return lastFile;
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
      } else if (validateAndSaveBlsResponse("state_industry", year, cacheParams, jsonFilePath, rawJson)) {
        lastFile = new File(jsonFilePath);
      }
    }

    return lastFile;
  }

  public File downloadStateWages() throws IOException, InterruptedException {
    return downloadStateWages(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads average weekly wages for all 51 U.S. jurisdictions (50 states + DC)
   * from BLS QCEW (Quarterly Census of Employment and Wages) CSV files.
   *
   * <p>Note: QCEW (ENU series) data is not available through the BLS API v2.
   * This method downloads annual QCEW CSV files and extracts state-level wage data.
   * Uses agglvl_code 50 for state-level aggregation.
   */
  public File downloadStateWages(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading state wages from QCEW CSV files for {}-{}", startYear, endYear);
    File lastFile = null;

    for (int year = startYear; year <= endYear; year++) {
      // QCEW data only available from 1990 forward
      if (year < 1990) {
        LOGGER.warn("QCEW data only available from 1990 forward. Skipping year {}", year);
        continue;
      }

      String outputDirPath = buildPartitionPath("state_wages", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/state_wages.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("state_wages", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached state wages for year {} - skipping", year);
        lastFile = new File(jsonFilePath);
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
        lastFile = new File(jsonFilePath);
        LOGGER.info("Extracted state wages for year {} ({} states)", year, STATE_FIPS_MAP.size());
      }
    }

    return lastFile;
  }

  public File downloadCountyWages() throws IOException, InterruptedException {
    return downloadCountyWages(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads county-level wage data from QCEW annual CSV files and converts to Parquet.
   * Extracts data for ~6,038 counties (most granular wage data available).
   * Reuses the same QCEW CSV files already downloaded for state wages.
   */
  public File downloadCountyWages(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading county wages from QCEW CSV files for {}-{}", startYear, endYear);
    File lastFile = null;

    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = buildPartitionPath("county_wages", DataFrequency.QUARTERLY, year);
      String jsonFilePath = outputDirPath + "/county_wages.json";
      Map<String, String> cacheParams = new HashMap<>();

      // Check if file is already in cache and up to date
      if (isCachedOrExists("county_wages", year, cacheParams, jsonFilePath)) {
        LOGGER.info("County wages data for year {} is already cached", year);
        lastFile = new File(jsonFilePath);
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
        lastFile = new File(jsonFilePath);
        LOGGER.info("Extracted county wages for year {}", year);
      }
    }

    return lastFile;
  }

  public File downloadCountyQcew() throws IOException, InterruptedException {
    return downloadCountyQcew(getDefaultStartYear(), getDefaultEndYear());
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
   * @param endYear End year (inclusive)
   * @return Last File object created, or null if none created
   * @throws IOException if download or parsing fails
   * @throws InterruptedException if interrupted
   */
  public File downloadCountyQcew(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading county QCEW data from BLS CSV files for {}-{}", startYear, endYear);
    File lastFile = null;

    for (int year = startYear; year <= endYear; year++) {
      String fullParquetPath = storageProvider.resolvePath(parquetDirectory,
          "type=county_qcew/frequency=quarterly/year=" + year + "/county_qcew.parquet");

      // Check if already exists
      if (storageProvider.exists(fullParquetPath)) {
        LOGGER.info("County QCEW data for year {} already exists - skipping", year);
        lastFile = new File("type=county_qcew/frequency=quarterly/year=" + year + "/county_qcew.parquet");
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
      lastFile = new File("type=county_qcew/frequency=quarterly/year=" + year + "/county_qcew.parquet");
      LOGGER.info("Completed county QCEW data for year {}", year);
    }

    return lastFile;
  }

  public File downloadMetroIndustryEmployment() throws IOException, InterruptedException {
    return downloadMetroIndustryEmployment(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads employment by industry data for 27 major U.S. metropolitan areas
   * across 22 NAICS supersector codes. Generates 594 series (27 × 22).
   *
   * <p>Optimized with year-batching to reduce API calls from ~180 (12 batches × 15 years)
   * to ~24 (12 batches × 2 year-batches).
   */
  public File downloadMetroIndustryEmployment(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading metro industry employment for {} metros × {} sectors ({} series) for {}-{}",
                METRO_AREA_CODES.size(), NAICS_SUPERSECTORS.size(),
                METRO_AREA_CODES.size() * NAICS_SUPERSECTORS.size(), startYear, endYear);

    File lastFile = null;
    List<String> seriesIds = Series.getAllMetroIndustryEmploymentSeriesIds();
    LOGGER.info("Generated {} metro industry employment series IDs", seriesIds.size());
    LOGGER.info("Metro industry series examples: {}, {}, {}",
                seriesIds.size() > 0 ? seriesIds.get(0) : "none",
                seriesIds.size() > 1 ? seriesIds.get(1) : "none",
                seriesIds.size() > 2 ? seriesIds.get(2) : "none");

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String jsonFilePath = buildPartitionPath("metro_industry", DataFrequency.QUARTERLY, year) + "/metro_industry.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("metro_industry", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached metro industry employment for year {} - skipping", year);
        lastFile = new File(jsonFilePath);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All metro industry employment data cached (years {}-{})", startYear, endYear);
      return lastFile;
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
      } else if (validateAndSaveBlsResponse("metro_industry", year, cacheParams, jsonFilePath, rawJson)) {
        lastFile = new File(jsonFilePath);
      }
    }

    return lastFile;
  }

  public File downloadMetroWages() throws IOException, InterruptedException {
    return downloadMetroWages(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads average weekly wages for 27 major U.S. metropolitan areas
   * from BLS QCEW (Quarterly Census of Employment and Wages) Open Data API.
   *
   * <p>Note: QCEW (ENU series) data is not available through the BLS API v2.
   * This method downloads quarterly QCEW CSV files from the Open Data API
   * and aggregates them to annual averages for metro-level wage data.
   * Uses agglvl_code 40 for MSA-level aggregation (Total All Ownerships).
   * QCEW aggregation codes: 4x=MSA, 5x=State, 7x=County.
   *
   * <p>Data is fetched from: https://data.bls.gov/cew/data/api/{year}/{qtr}/area/{area_code}.csv
   * where area_code is "C" + first 4 digits of CBSA code (e.g., C3562 for NYC).
   */
  public File downloadMetroWages(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading metro wages from QCEW Open Data API for {}-{}", startYear, endYear);
    File lastFile = null;

    for (int year = startYear; year <= endYear; year++) {
      // QCEW data only available from 1990 forward
      if (year < 1990) {
        LOGGER.warn("QCEW data only available from 1990 forward. Skipping year {}", year);
        continue;
      }

      String jsonFilePath = buildPartitionPath("metro_wages", DataFrequency.MONTHLY, year) + "/metro_wages.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("metro_wages", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached metro wages for year {} - skipping", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

      // Download quarterly data from QCEW Open Data API for all 27 metros
      String metroWagesJson = downloadQcewMetroWagesFromApi(year);

      if (metroWagesJson != null) {
        saveToCache("metro_wages", year, cacheParams, jsonFilePath, metroWagesJson);
        lastFile = new File(jsonFilePath);
        LOGGER.info("Downloaded metro wages for year {} ({} metros)", year, METRO_QCEW_AREA_CODES.size());
      } else {
        LOGGER.warn("Failed to download metro wages for year {}", year);
      }
    }

    return lastFile;
  }

  /**
   * Downloads metro wage data from QCEW Open Data API for a specific year.
   * Fetches quarterly data for all 27 metros and aggregates to annual averages.
   *
   * @param year The year to download data for
   * @return JSON array string containing metro wage records, or null if download fails
   */
  private String downloadQcewMetroWagesFromApi(int year) throws IOException, InterruptedException {
    List<Map<String, Object>> allMetroWages = new ArrayList<>();

    LOGGER.info("Downloading QCEW metro wages from API for {} metros in year {}",
        METRO_QCEW_AREA_CODES.size(), year);

    // Process each metro area
    for (Map.Entry<String, String> entry : METRO_QCEW_AREA_CODES.entrySet()) {
      String publicationCode = entry.getKey();  // e.g., "A100"
      String qcewAreaCode = entry.getValue();   // e.g., "C3562"
      String metroName = METRO_AREA_CODES.get(publicationCode);

      if (metroName == null) {
        LOGGER.warn("No metro name found for publication code: {}", publicationCode);
        continue;
      }

      // Download and aggregate quarterly wage data
      Integer annualAvgWage = downloadQcewQuarterlyWages(qcewAreaCode, year);

      if (annualAvgWage != null) {
        Map<String, Object> wageData = new HashMap<>();
        wageData.put("metro_area_code", publicationCode);
        wageData.put("metro_area_name", metroName);
        wageData.put("average_weekly_wage", annualAvgWage);
        wageData.put("year", year);

        allMetroWages.add(wageData);
        LOGGER.debug("Successfully downloaded wage data for {}: ${}/week", metroName, annualAvgWage);
      } else {
        LOGGER.warn("Failed to download wage data for {} ({})", metroName, qcewAreaCode);
      }
    }

    if (allMetroWages.isEmpty()) {
      LOGGER.error("No metro wage data downloaded for year {}", year);
      return null;
    }

    LOGGER.info("Successfully downloaded wage data for {} of {} metros",
        allMetroWages.size(), METRO_QCEW_AREA_CODES.size());

    // Convert to JSON array format
    try {
      return MAPPER.writeValueAsString(allMetroWages);
    } catch (Exception e) {
      LOGGER.error("Failed to serialize metro wages to JSON: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Downloads quarterly QCEW wage data for a specific metro area and calculates annual average.
   *
   * @param qcewAreaCode QCEW area code (e.g., "C3562" for NYC)
   * @param year Year to download
   * @return Annual average weekly wage, or null if data incomplete
   */
  private Integer downloadQcewQuarterlyWages(String qcewAreaCode, int year)
      throws IOException, InterruptedException {
    List<Integer> quarterlyWages = new ArrayList<>();

    // Download data for all 4 quarters
    for (int quarter = 1; quarter <= 4; quarter++) {
      String url =
          String.format("https://data.bls.gov/cew/data/api/%d/%d/area/%s.csv", year, quarter, qcewAreaCode);

      try {
        byte[] csvData = downloadFile(url);

        if (csvData == null) {
          LOGGER.warn("Failed to download QCEW data: {} Q{}", qcewAreaCode, quarter);
          continue;
        }

        // Parse CSV and extract avg_wkly_wage for agglvl=40, own=0, industry=10
        Integer wage = parseQcewCsvForWage(csvData);

        if (wage != null) {
          quarterlyWages.add(wage);
        }
      } catch (IOException e) {
        LOGGER.warn("Error downloading QCEW Q{} for {}: {}", quarter, qcewAreaCode, e.getMessage());
      }
    }

    // Need at least 3 quarters for reliable annual average
    if (quarterlyWages.size() < 3) {
      LOGGER.warn("Insufficient quarterly data for {} (only {} quarters)", qcewAreaCode, quarterlyWages.size());
      return null;
    }

    // Calculate annual average
    int sum = 0;
    for (Integer wage : quarterlyWages) {
      sum += wage;
    }
    return sum / quarterlyWages.size();
  }

  /**
   * Parses QCEW quarterly CSV and extracts average weekly wage.
   * Filters for agglvl_code=40 (MSA Total), own_code=0 (All), industry_code=10 (Total).
   *
   * @param csvData Raw CSV data bytes
   * @return Average weekly wage, or null if not found
   */
  private Integer parseQcewCsvForWage(byte[] csvData) throws IOException {
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(csvData), StandardCharsets.UTF_8))) {

      String line;
      boolean isHeader = true;

      while ((line = reader.readLine()) != null) {
        if (isHeader) {
          isHeader = false;
          continue;
        }

        String[] fields = parseCsvLine(line);
        if (fields.length < 16) continue;  // Need at least 16 fields to access index 15

        String areaFips = fields[0].trim();
        String ownCode = fields[1].trim();
        String industryCode = fields[2].trim();
        String agglvlCode = fields[3].trim();

        // Filter: MSA level (40), all ownership (0), total industry (10)
        if (agglvlCode.equals("40") && ownCode.equals("0") && industryCode.equals("10")) {
          // Field 15 = avg_wkly_wage (0-indexed: area_fips, own_code, industry_code, agglvl_code,
          // size_code, year, qtr, disclosure_code, qtrly_estabs, month1_emplvl, month2_emplvl,
          // month3_emplvl, total_qtrly_wages, taxable_qtrly_wages, qtrly_contributions, avg_wkly_wage)
          Integer wage = parseIntOrNull(fields[15]);
          if (wage != null) {
            return wage;
          }
        }
      }
    }

    return null;
  }

  public File downloadJoltsRegional() throws IOException, InterruptedException {
    return downloadJoltsRegional(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads JOLTS (Job Openings and Labor Turnover Survey) regional data from BLS FTP flat files.
   * Regional data is NOT available via BLS API v2 - must use download.bls.gov flat files.
   * Covers 4 Census regions (Northeast, Midwest, South, West) with 5 metrics each (20 series).
   */
  public File downloadJoltsRegional(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading JOLTS regional data from BLS FTP flat files for {}-{}", startYear, endYear);
    File lastFile = null;

    for (int year = startYear; year <= endYear; year++) {
      if (year < 2001) {
        LOGGER.warn("JOLTS data only available from 2001 forward. Skipping year {}", year);
        continue;
      }

      String jsonFilePath = buildPartitionPath("jolts_regional", DataFrequency.MONTHLY, year) + "/jolts_regional.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("jolts_regional", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached JOLTS regional for year {} - skipping", year);
        lastFile = new File(jsonFilePath);
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
        cacheStorageProvider.writeFile(fullJsonPath, joltsRegionalJson.getBytes(StandardCharsets.UTF_8));
        saveToCache("jolts_regional", year, cacheParams, jsonFilePath, "");
        lastFile = new File(jsonFilePath);
        LOGGER.info("Extracted JOLTS regional data for year {} (4 regions × 5 metrics)", year);
      }
    }

    return lastFile;
  }

  public File downloadJoltsState() throws IOException, InterruptedException {
    return downloadJoltsState(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads JOLTS state-level data from BLS FTP flat files and converts to Parquet.
   * Extracts data for all 51 states (including DC) for 5 metrics (job openings, hires, separations, quits, layoffs).
   */
  public File downloadJoltsState(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading JOLTS state data from BLS FTP flat files for {}-{}", startYear, endYear);
    File lastFile = null;

    for (int year = startYear; year <= endYear; year++) {
      String outputDirPath = buildPartitionPath("jolts_state", DataFrequency.MONTHLY, year);
      String jsonFilePath = outputDirPath + "/jolts_state.json";
      Map<String, String> cacheParams = new HashMap<>();

      // Check if file is already in cache and up to date
      if (isCachedOrExists("jolts_state", year, cacheParams, jsonFilePath)) {
        LOGGER.info("JOLTS state data for year {} is already cached", year);
        lastFile = new File(jsonFilePath);
        continue;
      }

      // Parse JOLTS FTP files for state data
      String joltsStateJson = parseJoltsFtpForState(year);

      if (joltsStateJson != null) {
        saveToCache("jolts_state", year, cacheParams, jsonFilePath, joltsStateJson);
        lastFile = new File(jsonFilePath);
        LOGGER.info("Extracted JOLTS state data for year {} (51 states × 5 metrics)", year);
      }
    }

    return lastFile;
  }

  /**
   * Downloads JOLTS industry code reference table from BLS FTP.
   * Downloads once (not partitioned by year).
   *
   * @return Reference file
   * @throws IOException if download fails
   * @throws InterruptedException if interrupted
   */
  public File downloadJoltsIndustries() throws IOException, InterruptedException {
    LOGGER.info("Downloading JOLTS industry reference data from BLS FTP");

    String outputDirPath = "source=econ/type=reference";
    String jsonFilePath = outputDirPath + "/jolts_industries.json";
    Map<String, String> cacheParams = new HashMap<>();

    // Check if already cached (use year=-1 for non-partitioned reference data)
    if (isCachedOrExists("jolts_industries", -1, cacheParams, jsonFilePath)) {
      LOGGER.info("JOLTS industry reference data already cached");
      return new File(jsonFilePath);
    }

    String url = "https://download.bls.gov/pub/time.series/jt/jt.industry";
    String ftpPath = "type=jolts_ftp/jt.industry";

    // Download file (will be cached by downloadJoltsFtpFileIfNeeded)
    byte[] data = downloadJoltsFtpFileIfNeeded(ftpPath, url);
    if (data == null) {
      LOGGER.warn("Failed to download JOLTS industry reference file");
      return null;
    }

    // Parse tab-delimited file
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
        industry.put("industry_code", fields[0].trim());
        industry.put("industry_name", fields[1].trim());
        industries.add(industry);
      }
    }

    LOGGER.info("Parsed {} JOLTS industries from reference file", industries.size());

    // Convert to JSON
    String json;
    try {
      json = MAPPER.writeValueAsString(industries);
    } catch (Exception e) {
      LOGGER.error("Failed to serialize JOLTS industries to JSON: {}", e.getMessage());
      return null;
    }

    // Save to cache (use year=-1 for non-partitioned reference data)
    saveToCache("jolts_industries", -1, cacheParams, jsonFilePath, json);
    return new File(jsonFilePath);
  }

  /**
   * Downloads JOLTS data element code reference table from BLS FTP.
   * Downloads once (not partitioned by year).
   *
   * @return Reference file
   * @throws IOException if download fails
   * @throws InterruptedException if interrupted
   */
  public File downloadJoltsDataelements() throws IOException, InterruptedException {
    LOGGER.info("Downloading JOLTS data element reference data from BLS FTP");

    String outputDirPath = "source=econ/type=reference";
    String jsonFilePath = outputDirPath + "/jolts_dataelements.json";
    Map<String, String> cacheParams = new HashMap<>();

    // Check if already cached (use year=-1 for non-partitioned reference data)
    if (isCachedOrExists("jolts_dataelements", -1, cacheParams, jsonFilePath)) {
      LOGGER.info("JOLTS data element reference data already cached");
      return new File(jsonFilePath);
    }

    String url = "https://download.bls.gov/pub/time.series/jt/jt.dataelement";
    String ftpPath = "type=jolts_ftp/jt.dataelement";

    // Download file (will be cached by downloadJoltsFtpFileIfNeeded)
    byte[] data = downloadJoltsFtpFileIfNeeded(ftpPath, url);
    if (data == null) {
      LOGGER.warn("Failed to download JOLTS data element reference file");
      return null;
    }

    // Parse tab-delimited file
    List<Map<String, Object>> dataElements = new ArrayList<>();
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

        Map<String, Object> dataElement = new HashMap<>();
        dataElement.put("dataelement_code", fields[0].trim());
        dataElement.put("dataelement_text", fields[1].trim());
        dataElements.add(dataElement);
      }
    }

    LOGGER.info("Parsed {} JOLTS data elements from reference file", dataElements.size());

    // Convert to JSON
    String json;
    try {
      json = MAPPER.writeValueAsString(dataElements);
    } catch (Exception e) {
      LOGGER.error("Failed to serialize JOLTS data elements to JSON: {}", e.getMessage());
      return null;
    }

    // Save to cache (use year=-1 for non-partitioned reference data)
    saveToCache("jolts_dataelements", -1, cacheParams, jsonFilePath, json);
    return new File(jsonFilePath);
  }

  public File downloadInflationMetrics() throws IOException, InterruptedException {
    return downloadInflationMetrics(getDefaultStartYear(), getDefaultEndYear());
  }

  /**
   * Downloads inflation metrics data and converts to Parquet.
   */
  public File downloadInflationMetrics(int startYear, int endYear) throws IOException, InterruptedException {
    File lastFile = null;

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

      if (isCachedOrExists("inflation_metrics", year, cacheParams, jsonFilePath)) {
        LOGGER.info("Found cached inflation metrics for year {} - skipping", year);
        lastFile = new File(jsonFilePath);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All inflation metrics data cached (years {}-{})", startYear, endYear);
      return lastFile;
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
      if (rawJson != null && validateAndSaveBlsResponse("inflation_metrics", year, cacheParams, jsonFilePath, rawJson)) {
        lastFile = new File(jsonFilePath);
      }
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
    File lastFile = null;

    List<String> seriesIds =
        List.of(Series.AVG_HOURLY_EARNINGS,
        Series.EMPLOYMENT_COST_INDEX);

    // 1. Identify uncached years
    List<Integer> uncachedYears = new ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      String relativePath = buildPartitionPath("wage_growth", DataFrequency.QUARTERLY, year) + "/wage_growth.json";
      Map<String, String> cacheParams = new HashMap<>();

      if (isCachedOrExists("wage_growth", year, cacheParams, relativePath)) {
        LOGGER.info("Found cached wage growth data for year {} - skipping", year);
        lastFile = new File(relativePath);
      } else {
        uncachedYears.add(year);
      }
    }

    if (uncachedYears.isEmpty()) {
      LOGGER.info("All wage growth data cached (years {}-{})", startYear, endYear);
      return lastFile;
    }

    // 2. Batch fetch uncached years
    LOGGER.info("Fetching wage growth for {} uncached years", uncachedYears.size());
    Map<Integer, String> resultsByYear = fetchAndSplitByYear(seriesIds, uncachedYears);

    // 3. Save each year
    for (int year : uncachedYears) {
      String relativePath = buildPartitionPath("wage_growth", DataFrequency.QUARTERLY, year) + "/wage_growth.json";
      Map<String, String> cacheParams = new HashMap<>();

      String rawJson = resultsByYear.get(year);
      if (rawJson != null && validateAndSaveBlsResponse("wage_growth", year, cacheParams, relativePath, rawJson)) {
        lastFile = new File(relativePath);
      }
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
   *
   * <p>Optimized to batch up to 20 years per API call (per state), reducing total API calls from
   * ~1,275 (51 states × 25 years) to ~102 (51 states × ~2 batches).
   */
  public File downloadRegionalEmployment(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading regional employment data for all 51 states/jurisdictions (years {}-{})", startYear, endYear);

    File lastFile = null;
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

        // Check cache manifest first
        if (cacheManifest.isParquetConverted("regional_employment", year, cacheParams)) {
          LOGGER.debug("State {} year {} already cached - skipping", stateName, year);
          totalFilesSkipped++;
          lastFile = new File(fullParquetPath);
          continue;
        }

        // Defensive check: if file exists but not in manifest, update manifest
        if (storageProvider.exists(fullParquetPath)) {
          LOGGER.info("State {} year {} parquet exists, updating manifest", stateName, year);
          cacheManifest.markParquetConverted("regional_employment", year, cacheParams, relativeParquetPath);
          cacheManifest.save(operatingDirectory);
          totalFilesSkipped++;
          lastFile = new File(fullParquetPath);
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

      // 3. Batch fetch uncached years (up to 20 years per API call)
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
          return lastFile;
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
          if (!seriesNode.isArray() || seriesNode.size() == 0) {
            LOGGER.warn("No series data for state {} year {} - skipping", stateName, year);
            continue;
          }

          // Convert JSON response to Parquet and save
          convertAndSaveRegionalEmployment(batchRoot, fullParquetPath, year, stateFips);

          // Mark as converted in manifest
          Map<String, String> cacheParams = new HashMap<>();
          cacheParams.put("state_fips", stateFips);
          cacheManifest.markParquetConverted("regional_employment", year, cacheParams, relativeParquetPath);
          cacheManifest.save(operatingDirectory);

          totalFilesDownloaded++;
          lastFile = new File(fullParquetPath);

          LOGGER.info("Saved state {} year {} ({} series)", stateName, year, seriesNode.size());

        } catch (Exception e) {
          LOGGER.warn("Failed to save state {} year {}: {}", stateName, year, e.getMessage());
          // Continue with next year
        }
      }
    }

    LOGGER.info("Regional employment download complete: {} files downloaded, {} already cached",
                totalFilesDownloaded, totalFilesSkipped);

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
      String relativePath = "type=county/year=" + year + "/county_employment.json";

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
   * Downloads a file from a URL and returns the bytes.
   *
   * <p>For downloads from download.bls.gov (BLS FTP site), uses browser-like headers
   * to bypass Akamai bot detection. For other URLs, uses default Java HTTP client.
   *
   * @param url URL to download from
   * @return File contents as byte array
   * @throws IOException if download fails
   */
  private byte[] downloadFile(String url) throws IOException {
    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMinutes(10)) // Large files may take time
        .GET();

    // BLS FTP site (download.bls.gov) blocks default Java HTTP client via Akamai
    // Add browser-like headers to bypass bot detection
    if (url.contains("download.bls.gov")) {
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

                // Log first few records for debugging
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
   * Downloads QCEW CSV file if not already cached.
   * Reuses cached data if available to avoid redundant downloads.
   *
   * @param year Year to download
   * @param qcewZipPath Relative path for caching
   * @return ZIP file bytes, or null if download fails
   */
  private byte[] downloadQcewCsvIfNeeded(int year, String qcewZipPath) throws IOException {
    // Check cache manifest first
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
    byte[] zipData = downloadFile(url);

    // Cache for reuse - use cacheStorageProvider for intermediate files
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, qcewZipPath);
    cacheStorageProvider.writeFile(fullPath, zipData);

    // Mark in cache manifest - QCEW data is immutable (historical), never refresh
    long refreshAfter = Long.MAX_VALUE;
    cacheManifest.markCached("qcew_zip", year, cacheParams, qcewZipPath, zipData.length, refreshAfter, "immutable_historical");
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
   * Parses QCEW CSV and extracts metro-level wage data.
   * Filters for agglvl_code = 40 (MSA Total All Ownerships), own_code = 0, industry_code = 10.
   * QCEW aggregation codes: 4x=MSA, 5x=State, 7x=County.
   *
   * @param zipData QCEW CSV ZIP file bytes
   * @param year Year of data
   * @return JSON string in BLS API response format, or null if parsing fails
   */
  private String parseQcewForMetroWages(byte[] zipData, int year) throws IOException {
    Map<String, Map<String, Object>> metroWages = new HashMap<>();

    // QCEW CSV uses area codes in format: State(2) + Area(5) = 7 digits
    // Build reverse map of QCEW area codes to metro names
    Map<String, String> qcewAreaToMetroName = new HashMap<>();
    for (Map.Entry<String, String> entry : METRO_BLS_AREA_CODES.entrySet()) {
      String publicationCode = entry.getKey();  // e.g., "A100"
      String qcewAreaCode = entry.getValue();   // e.g., "3693561" (State 36 + Area 93561)
      String metroName = METRO_AREA_CODES.get(publicationCode);
      if (metroName != null) {
        qcewAreaToMetroName.put(qcewAreaCode, metroName);
      }
    }

    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipData))) {
      ZipEntry entry;

      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().endsWith(".csv")) {
          // Don't use try-with-resources for BufferedReader - it would close the ZipInputStream
          BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8));
          String line;
          boolean isHeader = true;
          int debugCount = 0;

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

            // Filter: MSA level (40), all ownership (0), total industry (10)
            // QCEW aggregation codes: 4x=MSA, 5x=State, 7x=County
            // 40 = MSA Total All Ownerships, 44 = MSA Sector by Ownership
            if (!agglvlCode.equals("40") || !ownCode.equals("0") || !industryCode.equals("10")) {
              continue;
            }

            // Log first 10 MSA records to see actual area codes
            if (debugCount < 10) {
              LOGGER.info("QCEW Metro Debug: areaFips={}, agglvlCode={}, ownCode={}, industryCode={}",
                  areaFips, agglvlCode, ownCode, industryCode);
              debugCount++;
            }

            // Check if this is one of our tracked metros
            String metroName = qcewAreaToMetroName.get(areaFips);
            if (metroName == null) {
              continue;
            }

            // QCEW CSV fields: [13]=annual_avg_wkly_wage, [9]=annual_avg_emplvl
            Integer avgWklyWage = parseIntOrNull(fields[13]);
            Integer avgEmplvl = parseIntOrNull(fields[9]);

            if (avgWklyWage != null) {
              Map<String, Object> wageData = new HashMap<>();
              wageData.put("metro_area_code", areaFips);
              wageData.put("metro_area_name", metroName);
              wageData.put("average_weekly_wage", avgWklyWage);
              wageData.put("total_employment", avgEmplvl);
              wageData.put("year", year);

              metroWages.put(areaFips, wageData);
            }
          }
          zis.closeEntry();
        }
      }
    }

    if (metroWages.isEmpty()) {
      LOGGER.warn("No metro wage data found in QCEW CSV for year {}", year);
      return null;
    }

    LOGGER.info("Extracted wage data for {} metros from QCEW CSV (year {})", metroWages.size(), year);

    // Convert to JSON array format
    try {
      return MAPPER.writeValueAsString(new ArrayList<>(metroWages.values()));
    } catch (Exception e) {
      LOGGER.error("Failed to serialize metro wages to JSON: {}", e.getMessage());
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
    // Extract file name for cache key (e.g., "jt.series" from "type=jolts_ftp/jt.series")
    String fileName = ftpPath.substring(ftpPath.lastIndexOf('/') + 1);
    String dataType = "jolts_ftp_" + fileName.replace(".", "_");

    // Check cache manifest first (use year=0 for non-year-partitioned files)
    Map<String, String> cacheParams = new HashMap<>();
    cacheParams.put("file", fileName);
    if (cacheManifest.isCached(dataType, 0, cacheParams)) {
      String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, ftpPath);
      if (cacheStorageProvider.exists(fullPath)) {
        LOGGER.info("Using cached JOLTS FTP file: {} (from manifest)", ftpPath);
        try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(fullPath)) {
          return inputStream.readAllBytes();
        }
      } else {
        LOGGER.warn("Cache manifest lists JOLTS FTP file {} but file not found - re-downloading", fileName);
      }
    }

    LOGGER.info("Downloading JOLTS FTP file from {}", url);
    byte[] data = downloadFile(url);

    // Cache for reuse - use cacheStorageProvider for intermediate files
    String fullPath = cacheStorageProvider.resolvePath(cacheDirectory, ftpPath);
    cacheStorageProvider.writeFile(fullPath, data);

    // Mark in cache manifest - refresh monthly (JOLTS data updates monthly with ~2 month lag)
    long refreshAfter = System.currentTimeMillis() + (30L * 24 * 60 * 60 * 1000); // 30 days in milliseconds
    cacheManifest.markCached(dataType, 0, cacheParams, ftpPath, data.length, refreshAfter, "monthly_refresh");
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

          // Log first 5 records and any matching records for debugging
          if (debugSamples < 5 || (yearStr.equals(String.valueOf(year)) && period.equals("M13"))) {
            LOGGER.info("JOLTS Debug [{}]: seriesId={}, year={}, period={}, value={}, state_code[9-10]={}",
                debugSamples, seriesId, yearStr, period, valueStr,
                seriesId.length() >= 11 ? seriesId.substring(9, 11) : "N/A");
            if (debugSamples < 5) debugSamples++;
          }

          // Check if this is a regional series and matches our target year
          if (yearStr.equals(String.valueOf(year)) && period.equals("M13")) { // Use annual average (M13)
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

  private void writeRegionalCpiParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("regional_cpi")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("series_id").doc("BLS series identifier").type().stringType().noDefault()
        .name("area_code").doc("Census region code").type().stringType().noDefault()
        .name("area_name").doc("Census region name").type().stringType().noDefault()
        .name("value").doc("CPI value (base period = 100)").type().doubleType().noDefault()
        .name("percent_change_month").doc("Month-over-month percentage change").type().nullable().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change").type().nullable().doubleType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("area_code", extractAreaCodeFromSeries(seriesId));
        record.put("area_name", extractAreaNameFromSeries(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private void writeMetroCpiParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("metro_cpi")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("series_id").doc("BLS series identifier").type().stringType().noDefault()
        .name("area_code").doc("Metro area code").type().stringType().noDefault()
        .name("area_name").doc("Metro area name").type().stringType().noDefault()
        .name("value").doc("CPI value (base period = 100)").type().doubleType().noDefault()
        .name("percent_change_month").doc("Month-over-month percentage change").type().nullable().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change").type().nullable().doubleType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("area_code", extractMetroCodeFromSeries(seriesId));
        record.put("area_name", extractMetroNameFromSeries(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private void writeStateIndustryParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("state_industry")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("series_id").doc("BLS series identifier").type().stringType().noDefault()
        .name("state_code").doc("State FIPS code").type().stringType().noDefault()
        .name("state_name").doc("State name").type().stringType().noDefault()
        .name("industry_code").doc("NAICS supersector code").type().stringType().noDefault()
        .name("industry_name").doc("Industry name").type().stringType().noDefault()
        .name("value").doc("Employment in thousands").type().doubleType().noDefault()
        .name("percent_change_month").doc("Month-over-month percentage change").type().nullable().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change").type().nullable().doubleType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("state_code", extractStateCodeFromSeries(seriesId));
        record.put("state_name", extractStateNameFromSeries(seriesId));
        record.put("industry_code", extractIndustryCodeFromSeries(seriesId));
        record.put("industry_name", extractIndustryNameFromSeries(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private void writeStateWagesParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("state_wages")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("series_id").doc("BLS series identifier").type().stringType().noDefault()
        .name("state_code").doc("State FIPS code").type().stringType().noDefault()
        .name("state_name").doc("State name").type().stringType().noDefault()
        .name("value").doc("Average weekly wage in dollars").type().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change").type().nullable().doubleType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("state_code", extractStateCodeFromSeries(seriesId));
        record.put("state_name", extractStateNameFromSeries(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private void writeStateWagesQcewParquet(List<Map<String, Object>> records, String targetPath) throws IOException {
    // Schema for QCEW-based state wages data (flat array format)
    Schema schema = SchemaBuilder.record("state_wages")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("year").doc("Year of observation").type().intType().noDefault()
        .name("state_fips").doc("State FIPS code").type().stringType().noDefault()
        .name("state_name").doc("State name").type().stringType().noDefault()
        .name("average_weekly_wage").doc("Average weekly wage in dollars").type().doubleType().noDefault()
        .name("total_employment").doc("Total employment count").type().intType().noDefault()
        .endRecord();

    List<GenericRecord> avroRecords = new ArrayList<>();
    for (Map<String, Object> record : records) {
      GenericRecord avroRecord = new GenericData.Record(schema);
      avroRecord.put("year", record.get("year"));
      avroRecord.put("state_fips", record.get("state_fips"));
      avroRecord.put("state_name", record.get("state_name"));
      avroRecord.put("average_weekly_wage", record.get("average_weekly_wage"));
      avroRecord.put("total_employment", record.get("total_employment"));
      avroRecords.add(avroRecord);
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, avroRecords, schema.getName());
  }

  private void writeMetroIndustryParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("metro_industry")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("series_id").doc("BLS series identifier").type().stringType().noDefault()
        .name("metro_code").doc("Metro area code").type().stringType().noDefault()
        .name("metro_name").doc("Metro area name").type().stringType().noDefault()
        .name("industry_code").doc("NAICS supersector code").type().stringType().noDefault()
        .name("industry_name").doc("Industry name").type().stringType().noDefault()
        .name("value").doc("Employment in thousands").type().doubleType().noDefault()
        .name("percent_change_month").doc("Month-over-month percentage change").type().nullable().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change").type().nullable().doubleType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("metro_code", extractMetroCodeFromSeries(seriesId));
        record.put("metro_name", extractMetroNameFromSeries(seriesId));
        record.put("industry_code", extractIndustryCodeFromSeries(seriesId));
        record.put("industry_name", extractIndustryNameFromSeries(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private void writeMetroWagesParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("metro_wages")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("series_id").doc("BLS series identifier").type().stringType().noDefault()
        .name("metro_code").doc("Metro area code").type().stringType().noDefault()
        .name("metro_name").doc("Metro area name").type().stringType().noDefault()
        .name("value").doc("Average weekly wage in dollars").type().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change").type().nullable().doubleType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("metro_code", extractMetroCodeFromSeries(seriesId));
        record.put("metro_name", extractMetroNameFromSeries(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private void writeMetroWagesQcewParquet(List<Map<String, Object>> records, String targetPath) throws IOException {
    // Schema for QCEW-based metro wages data (flat array format)
    Schema schema = SchemaBuilder.record("metro_wages")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("year").doc("Year of observation").type().intType().noDefault()
        .name("metro_area_code").doc("Metro area code").type().stringType().noDefault()
        .name("metro_area_name").doc("Metro area name").type().stringType().noDefault()
        .name("average_weekly_wage").doc("Average weekly wage in dollars").type().doubleType().noDefault()
        .name("total_employment").doc("Total employment count").type().nullable().intType().noDefault()
        .endRecord();

    List<GenericRecord> avroRecords = new ArrayList<>();
    for (Map<String, Object> record : records) {
      GenericRecord avroRecord = new GenericData.Record(schema);
      avroRecord.put("year", record.get("year"));
      avroRecord.put("metro_area_code", record.get("metro_area_code"));
      avroRecord.put("metro_area_name", record.get("metro_area_name"));
      avroRecord.put("average_weekly_wage", record.get("average_weekly_wage"));
      avroRecord.put("total_employment", record.get("total_employment"));
      avroRecords.add(avroRecord);
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, avroRecords, schema.getName());
  }

  private void writeJoltsRegionalParquet(Map<String, List<Map<String, Object>>> seriesData,
      String targetPath) throws IOException {

    Schema schema = SchemaBuilder.record("jolts_regional")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("date").doc("Observation date (ISO 8601 format)").type().stringType().noDefault()
        .name("series_id").doc("BLS series identifier").type().stringType().noDefault()
        .name("region_code").doc("Census region code").type().stringType().noDefault()
        .name("region_name").doc("Census region name").type().stringType().noDefault()
        .name("metric_type").doc("JOLTS metric (hires, separations, openings, quits, layoffs)").type().stringType().noDefault()
        .name("value").doc("Metric value in thousands").type().doubleType().noDefault()
        .name("percent_change_month").doc("Month-over-month percentage change").type().nullable().doubleType().noDefault()
        .name("percent_change_year").doc("Year-over-year percentage change").type().nullable().doubleType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : seriesData.entrySet()) {
      String seriesId = entry.getKey();
      for (Map<String, Object> dataPoint : entry.getValue()) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("date", dataPoint.get("date"));
        record.put("series_id", seriesId);
        record.put("region_code", extractRegionCodeFromJoltsSeries(seriesId));
        record.put("region_name", extractRegionNameFromJoltsSeries(seriesId));
        record.put("metric_type", extractJoltsMetricType(seriesId));
        record.put("value", dataPoint.get("value"));
        record.put("percent_change_month", dataPoint.get("percent_change_month"));
        record.put("percent_change_year", dataPoint.get("percent_change_year"));
        records.add(record);
      }
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private void writeCountyWagesParquet(List<Map<String, Object>> countyWages, String targetPath) throws IOException {
    Schema schema = SchemaBuilder.record("county_wages")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("county_fips").doc("County FIPS code").type().stringType().noDefault()
        .name("county_name").doc("County name").type().stringType().noDefault()
        .name("state_fips").doc("State FIPS code").type().stringType().noDefault()
        .name("state_name").doc("State name").type().stringType().noDefault()
        .name("average_weekly_wage").doc("Average weekly wage in dollars").type().intType().noDefault()
        .name("total_employment").doc("Total employment count").type().nullable().intType().noDefault()
        .name("year").doc("Year").type().intType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map<String, Object> countyWage : countyWages) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("county_fips", countyWage.get("county_fips"));
      record.put("county_name", countyWage.get("county_name"));
      record.put("state_fips", countyWage.get("state_fips"));
      record.put("state_name", countyWage.get("state_name"));
      record.put("average_weekly_wage", countyWage.get("average_weekly_wage"));
      record.put("total_employment", countyWage.get("total_employment"));
      record.put("year", countyWage.get("year"));
      records.add(record);
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private void writeJoltsStateParquet(List<Map<String, Object>> joltsState, String targetPath) throws IOException {
    Schema schema = SchemaBuilder.record("jolts_state")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("state_fips").doc("State FIPS code").type().stringType().noDefault()
        .name("state_name").doc("State name").type().stringType().noDefault()
        .name("year").doc("Year").type().intType().noDefault()
        .name("hires_rate").doc("Hires rate").type().doubleType().noDefault()
        .name("jobopenings_rate").doc("Job openings rate").type().doubleType().noDefault()
        .name("quits_rate").doc("Quits rate").type().doubleType().noDefault()
        .name("layoffsdischarges_rate").doc("Layoffs and discharges rate").type().doubleType().noDefault()
        .name("totalseparations_rate").doc("Total separations rate").type().doubleType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map<String, Object> jolts : joltsState) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("state_fips", jolts.get("state_fips"));
      record.put("state_name", jolts.get("state_name"));
      record.put("year", jolts.get("year"));
      record.put("hires_rate", jolts.get("hires_rate"));
      record.put("jobopenings_rate", jolts.get("jobopenings_rate"));
      record.put("quits_rate", jolts.get("quits_rate"));
      record.put("layoffsdischarges_rate", jolts.get("layoffsdischarges_rate"));
      record.put("totalseparations_rate", jolts.get("totalseparations_rate"));
      records.add(record);
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private void writeJoltsIndustriesParquet(List<Map<String, Object>> industries, String targetPath) throws IOException {
    Schema schema = SchemaBuilder.record("jolts_industries")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("industry_code").doc("JOLTS industry code").type().stringType().noDefault()
        .name("industry_name").doc("Industry name").type().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map<String, Object> industry : industries) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("industry_code", industry.get("industry_code"));
      record.put("industry_name", industry.get("industry_name"));
      records.add(record);
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
  }

  private void writeJoltsDataelementsParquet(List<Map<String, Object>> dataelements, String targetPath) throws IOException {
    Schema schema = SchemaBuilder.record("jolts_dataelements")
        .namespace("org.apache.calcite.adapter.govdata.econ")
        .fields()
        .name("dataelement_code").doc("JOLTS data element code").type().stringType().noDefault()
        .name("dataelement_text").doc("Data element description").type().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (Map<String, Object> dataelement : dataelements) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("dataelement_code", dataelement.get("dataelement_code"));
      record.put("dataelement_text", dataelement.get("dataelement_text"));
      records.add(record);
    }

    // Write parquet using StorageProvider
    storageProvider.writeAvroParquet(targetPath, schema, records, schema.getName());
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
    // Simplified metro name extraction - in production, use lookup map
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
    } else if (fileName.equals("regional_cpi.parquet")) {
      convertRegionalCpiToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("metro_cpi.parquet")) {
      convertMetroCpiToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("state_industry.parquet")) {
      convertStateIndustryToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("state_wages.parquet")) {
      convertStateWagesToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("metro_industry.parquet")) {
      convertMetroIndustryToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("metro_wages.parquet")) {
      convertMetroWagesToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("jolts_regional.parquet")) {
      convertJoltsRegionalToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("county_wages.parquet")) {
      convertCountyWagesToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("jolts_state.parquet")) {
      convertJoltsStateToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("jolts_industries.parquet")) {
      convertJoltsIndustriesToParquet(sourceDirPath, targetFilePath);
    } else if (fileName.equals("jolts_dataelements.parquet")) {
      convertJoltsDataelementsToParquet(sourceDirPath, targetFilePath);
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

  private void convertRegionalCpiToParquet(String sourceDirPath, String targetPath) throws IOException {
    Map<String, List<Map<String, Object>>> seriesData = new HashMap<>();

    // Read regional CPI JSON file from cache using cacheStorageProvider
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "regional_cpi.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No regional_cpi.json found in {}", sourceDirPath);
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
    String jsonFilePath = cacheStorageProvider.resolvePath(sourceDirPath, "metro_cpi.json");
    if (!cacheStorageProvider.exists(jsonFilePath)) {
      LOGGER.warn("No metro_cpi.json found in {}", sourceDirPath);
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

    List<Map<String, Object>> records = new ArrayList<>();
    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);

      // Check if root is an array (QCEW format)
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

    // Write to parquet using simplified state wages schema
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

    List<Map<String, Object>> records = new ArrayList<>();
    try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(jsonFilePath);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      JsonNode root = MAPPER.readTree(reader);

      // Check if root is an array (QCEW format)
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

    // Write to parquet using simplified metro wages schema
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

    List<Map<String, Object>> countyWages = new ArrayList<>();
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

    List<Map<String, Object>> joltsState = new ArrayList<>();
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

    List<Map<String, Object>> industries = new ArrayList<>();
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

    List<Map<String, Object>> dataelements = new ArrayList<>();
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
