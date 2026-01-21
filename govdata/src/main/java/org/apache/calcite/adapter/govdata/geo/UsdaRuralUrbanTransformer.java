/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Transforms USDA Economic Research Service Rural-Urban Continuum Codes CSV data.
 *
 * <p>The RUCC file is a CSV with columns like:
 * FIPS,State,County_Name,Population_2020,RUCC_2023,Description
 *
 * <p>This transformer converts CSV to JSON array format for Parquet conversion.
 */
public class UsdaRuralUrbanTransformer implements ResponseTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(UsdaRuralUrbanTransformer.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

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
        "Nonmetro - Completely rural or less than 2,500 urban population, adjacent to metro area");
    RUCC_DESCRIPTIONS.put(9,
        "Nonmetro - Completely rural or less than 2,500 urban population, not adjacent to metro");
  }

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Empty response body for USDA Rural-Urban data");
      return "[]";
    }

    try {
      ArrayNode result = JSON_MAPPER.createArrayNode();

      try (BufferedReader reader = new BufferedReader(new StringReader(response))) {
        String headerLine = reader.readLine();
        if (headerLine == null) {
          return "[]";
        }

        // Parse header to find column indices
        String[] headers = parseCSVLine(headerLine);
        int fipsIdx = findColumnIndex(headers, "FIPS", "fips", "GEOID");
        int stateIdx = findColumnIndex(headers, "State", "state", "STATE_CODE");
        int countyNameIdx = findColumnIndex(headers, "County_Name", "county_name", "NAME");
        int populationIdx = findColumnIndex(headers, "Population_2020", "population", "POP");
        int ruccIdx = findColumnIndex(headers, "RUCC_2023", "RUCC", "rucc_code");

        String line;
        while ((line = reader.readLine()) != null) {
          if (line.trim().isEmpty()) {
            continue;
          }

          String[] values = parseCSVLine(line);
          ObjectNode record = JSON_MAPPER.createObjectNode();

          String fips = getValueSafe(values, fipsIdx);
          if (fips != null && fips.length() >= 2) {
            // Pad FIPS to 5 digits if needed
            fips = String.format("%05d", Integer.parseInt(fips));
            record.put("county_fips", fips);
            record.put("state_fips", fips.substring(0, 2));
          }

          record.put("county_name", getValueSafe(values, countyNameIdx));

          String ruccStr = getValueSafe(values, ruccIdx);
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

          String popStr = getValueSafe(values, populationIdx);
          if (popStr != null && !popStr.isEmpty()) {
            try {
              record.put("population", Integer.parseInt(popStr.replace(",", "")));
            } catch (NumberFormatException e) {
              LOGGER.debug("Invalid population: {}", popStr);
            }
          }

          result.add(record);
        }
      }

      LOGGER.info("Transformed {} RUCC records", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("Error transforming USDA Rural-Urban data: {}", e.getMessage(), e);
      return "[]";
    }
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
        if (headers[i].equalsIgnoreCase(name) || headers[i].replace("\"", "").equalsIgnoreCase(name)) {
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
