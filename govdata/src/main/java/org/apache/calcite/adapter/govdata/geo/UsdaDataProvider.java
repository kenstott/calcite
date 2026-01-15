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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom DataProvider for USDA Economic Research Service data.
 *
 * <p>Handles USDA Rural-Urban Continuum Codes and RUCA data which are
 * provided in CSV format. The RUCC CSV uses a pivoted key-value format
 * where each county has multiple rows with different attributes.
 */
public class UsdaDataProvider implements DataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(UsdaDataProvider.class);

  /** RUCC code descriptions. */
  private static final Map<Integer, String> RUCC_DESCRIPTIONS = new HashMap<>();

  static {
    RUCC_DESCRIPTIONS.put(1, "Metro - Counties in metro areas of 1 million population or more");
    RUCC_DESCRIPTIONS.put(2, "Metro - Counties in metro areas of 250,000 to 1 million population");
    RUCC_DESCRIPTIONS.put(3, "Metro - Counties in metro areas of fewer than 250,000 population");
    RUCC_DESCRIPTIONS.put(4,
        "Nonmetro - Urban population of 20,000 or more, adjacent to a metro area");
    RUCC_DESCRIPTIONS.put(5,
        "Nonmetro - Urban population of 20,000 or more, not adjacent to a metro area");
    RUCC_DESCRIPTIONS.put(6,
        "Nonmetro - Urban population of 2,500 to 19,999, adjacent to a metro area");
    RUCC_DESCRIPTIONS.put(7,
        "Nonmetro - Urban population of 2,500 to 19,999, not adjacent to a metro area");
    RUCC_DESCRIPTIONS.put(8,
        "Nonmetro - Completely rural or less than 2,500 urban population, adjacent to metro");
    RUCC_DESCRIPTIONS.put(9,
        "Nonmetro - Completely rural or less than 2,500 urban population, not adjacent to metro");
  }

  @Override
  public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config, Map<String, String> variables)
      throws IOException {

    String tableName = config.getName();
    String year = variables.get("year");

    LOGGER.info("UsdaDataProvider: Fetching {} for year={}", tableName, year);

    String url = buildDownloadUrl(tableName, year);
    if (url == null) {
      LOGGER.warn("Could not build download URL for table {} year={}", tableName, year);
      return new ArrayList<Map<String, Object>>().iterator();
    }

    try {
      LOGGER.info("Downloading USDA data from: {}", url);
      String csvContent = downloadCsv(url);

      List<Map<String, Object>> result;
      switch (tableName) {
      case "rural_urban_continuum":
        result = parseRuralUrbanCsv(csvContent, year);
        break;
      case "ruca_codes":
        result = parseRucaCsv(csvContent, year);
        break;
      default:
        LOGGER.warn("Unknown USDA table: {}", tableName);
        result = new ArrayList<>();
      }

      LOGGER.info("Parsed {} records from USDA for table {}", result.size(), tableName);
      return result.iterator();

    } catch (IOException e) {
      throw e;
    }
  }

  private String buildDownloadUrl(String tableName, String year) {
    switch (tableName) {
    case "rural_urban_continuum":
      // USDA updates RUCC data periodically, use 2023 dataset for now
      return "https://www.ers.usda.gov/media/5768/2023-rural-urban-continuum-codes.csv";
    case "ruca_codes":
      // RUCA codes based on 2010 or 2020 census
      if (Integer.parseInt(year) >= 2020) {
        return "https://www.ers.usda.gov/media/5443/2020-rural-urban-commuting-area-codes-census-tracts.csv";
      } else {
        return "https://www.ers.usda.gov/media/5443/2020-rural-urban-commuting-area-codes-census-tracts.csv";
      }
    default:
      return null;
    }
  }

  private String downloadCsv(String urlString) throws IOException {
    URI uri = URI.create(urlString);
    URL url = uri.toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "Apache-Calcite-GovData-Adapter/1.0");
    conn.setRequestProperty("Accept", "text/csv, */*");

    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP " + responseCode + " for URL: " + urlString);
    }

    StringBuilder content = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new BufferedInputStream(conn.getInputStream()),
            StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        content.append(line).append("\n");
      }
    }
    return content.toString();
  }

  /**
   * Parses USDA Rural-Urban Continuum Codes CSV.
   *
   * <p>The CSV has a pivoted format: FIPS,State,County_Name,Attribute,Value
   * where each county has multiple rows for different attributes.
   */
  private List<Map<String, Object>> parseRuralUrbanCsv(String csvContent, String year)
      throws IOException {
    // First, pivot the data to get one record per county
    Map<String, Map<String, String>> countyData = new LinkedHashMap<>();

    try (BufferedReader reader = new BufferedReader(new java.io.StringReader(csvContent))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return new ArrayList<>();
      }

      // Parse header to find column indices
      String[] headers = parseCSVLine(headerLine);
      int fipsIdx = findColumnIndex(headers, "FIPS");
      int stateIdx = findColumnIndex(headers, "State");
      int countyNameIdx = findColumnIndex(headers, "County_Name");
      int attrIdx = findColumnIndex(headers, "Attribute");
      int valueIdx = findColumnIndex(headers, "Value");

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }

        String[] values = parseCSVLine(line);
        String fips = getValueSafe(values, fipsIdx);
        if (fips == null || fips.isEmpty()) {
          continue;
        }

        // Get or create county record
        Map<String, String> county = countyData.get(fips);
        if (county == null) {
          county = new HashMap<>();
          county.put("fips", fips);
          county.put("state", getValueSafe(values, stateIdx));
          county.put("county_name", getValueSafe(values, countyNameIdx));
          countyData.put(fips, county);
        }

        // Add attribute value
        String attr = getValueSafe(values, attrIdx);
        String value = getValueSafe(values, valueIdx);
        if (attr != null && value != null) {
          county.put(attr, value);
        }
      }
    }

    // Convert pivoted data to records
    List<Map<String, Object>> result = new ArrayList<>();
    for (Map<String, String> county : countyData.values()) {
      Map<String, Object> record = new HashMap<>();

      // Add partition columns
      record.put("type", "classification");
      record.put("year", Integer.parseInt(year));

      String fips = county.get("fips");
      if (fips != null && !fips.isEmpty()) {
        // Pad FIPS to 5 digits
        fips = String.format("%05d", Integer.parseInt(fips));
        record.put("county_fips", fips);
        record.put("state_fips", fips.substring(0, 2));
      }

      record.put("county_name", county.get("county_name"));

      // Parse RUCC code
      String ruccStr = county.get("RUCC_2023");
      if (ruccStr == null) {
        ruccStr = county.get("RUCC_2013");
      }
      if (ruccStr != null && !ruccStr.isEmpty()) {
        try {
          int rucc = Integer.parseInt(ruccStr);
          record.put("rucc_code", rucc);
          record.put("rucc_description", RUCC_DESCRIPTIONS.get(rucc));
          record.put("metro_nonmetro", rucc <= 3 ? "Metro" : "Nonmetro");
        } catch (NumberFormatException e) {
          LOGGER.debug("Invalid RUCC code: {}", ruccStr);
        }
      }

      // Parse population
      String popStr = county.get("Population_2020");
      if (popStr == null) {
        popStr = county.get("Population_2010");
      }
      if (popStr != null && !popStr.isEmpty()) {
        try {
          record.put("population", Integer.parseInt(popStr.replace(",", "")));
        } catch (NumberFormatException e) {
          LOGGER.debug("Invalid population: {}", popStr);
        }
      }

      result.add(record);
    }

    return result;
  }

  /**
   * Parses USDA RUCA codes CSV.
   *
   * <p>RUCA file format varies but typically has tract-level data with
   * primary and secondary RUCA codes.
   */
  private List<Map<String, Object>> parseRucaCsv(String csvContent, String year)
      throws IOException {
    List<Map<String, Object>> result = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new java.io.StringReader(csvContent))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return result;
      }

      // Parse header
      String[] headers = parseCSVLine(headerLine);
      int tractIdx = findColumnIndex(headers, "TRACTFIPS", "tract", "GEOID", "STATE_COUNTY_TRACT_FIPS_CODE");
      int stateIdx = findColumnIndex(headers, "State", "STATE_CODE", "state_fips");
      int countyIdx = findColumnIndex(headers, "County", "COUNTY_FIPS_CODE", "county_fips");
      int primaryIdx = findColumnIndex(headers, "Primary_RUCA_Code_2010", "RUCA1", "PRIMARY_RUCA_CODE_2010");
      int secondaryIdx = findColumnIndex(headers, "Secondary_RUCA_Code_2010", "RUCA2", "SECONDARY_RUCA_CODE_2010");
      int popIdx = findColumnIndex(headers, "Tract_Population_2010", "Population", "TRACT_POPULATION");
      int landAreaIdx = findColumnIndex(headers, "Land_Area", "LAND_AREA_SQUARE_MILES");

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }

        String[] values = parseCSVLine(line);
        Map<String, Object> record = new HashMap<>();

        // Add partition columns
        record.put("type", "classification");
        record.put("year", Integer.parseInt(year));

        String tractFips = getValueSafe(values, tractIdx);
        if (tractFips != null && !tractFips.isEmpty()) {
          // Pad tract FIPS to 11 digits
          tractFips = String.format("%011d", Long.parseLong(tractFips.replace(" ", "").replace("-", "")));
          record.put("tract_fips", tractFips);
          if (tractFips.length() >= 5) {
            record.put("state_fips", tractFips.substring(0, 2));
            record.put("county_fips", tractFips.substring(0, 5));
          }
        }

        String primaryCode = getValueSafe(values, primaryIdx);
        if (primaryCode != null && !primaryCode.isEmpty()) {
          try {
            record.put("primary_ruca_code", Double.valueOf(primaryCode).intValue());
          } catch (NumberFormatException e) {
            LOGGER.debug("Invalid primary RUCA code: {}", primaryCode);
          }
        }

        String secondaryCode = getValueSafe(values, secondaryIdx);
        if (secondaryCode != null && !secondaryCode.isEmpty()) {
          try {
            record.put("secondary_ruca_code", Double.valueOf(secondaryCode));
          } catch (NumberFormatException e) {
            LOGGER.debug("Invalid secondary RUCA code: {}", secondaryCode);
          }
        }

        String popStr = getValueSafe(values, popIdx);
        if (popStr != null && !popStr.isEmpty()) {
          try {
            record.put("tract_population", Integer.parseInt(popStr.replace(",", "")));
          } catch (NumberFormatException e) {
            LOGGER.debug("Invalid population: {}", popStr);
          }
        }

        String landArea = getValueSafe(values, landAreaIdx);
        if (landArea != null && !landArea.isEmpty()) {
          try {
            record.put("land_area_sqmi", Double.parseDouble(landArea));
          } catch (NumberFormatException e) {
            LOGGER.debug("Invalid land area: {}", landArea);
          }
        }

        result.add(record);
      }
    }

    return result;
  }

  private String[] parseCSVLine(String line) {
    // Simple CSV parser - handles quoted fields with commas
    java.util.List<String> values = new java.util.ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '"') {
        inQuotes = !inQuotes;
      } else if (c == ',' && !inQuotes) {
        values.add(current.toString().trim());
        current = new StringBuilder();
      } else {
        current.append(c);
      }
    }
    values.add(current.toString().trim());

    return values.toArray(new String[0]);
  }

  private int findColumnIndex(String[] headers, String... names) {
    for (String name : names) {
      for (int i = 0; i < headers.length; i++) {
        if (headers[i].equalsIgnoreCase(name)
            || headers[i].replace("\"", "").equalsIgnoreCase(name)) {
          return i;
        }
      }
    }
    return -1;
  }

  private String getValueSafe(String[] values, int index) {
    if (index >= 0 && index < values.length) {
      String value = values[index].replace("\"", "").trim();
      return value.isEmpty() ? null : value;
    }
    return null;
  }
}
