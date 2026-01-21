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
 * Transforms USDA Rural-Urban Commuting Area (RUCA) codes CSV data.
 *
 * <p>RUCA codes classify census tracts by urbanization and commuting patterns.
 * Primary codes (1-10) with secondary subdivision codes.
 *
 * <p>The RUCA file is typically CSV with columns:
 * STATE_FIPS,COUNTY_FIPS,TRACT_FIPS,PRIMARY_RUCA_CODE_2020,SECONDARY_RUCA_CODE_2020,POPULATION
 */
public class UsdaRucaTransformer implements ResponseTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(UsdaRucaTransformer.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  /** RUCA primary code descriptions. */
  private static final Map<Integer, String> RUCA_DESCRIPTIONS = new HashMap<>();

  static {
    RUCA_DESCRIPTIONS.put(1, "Metropolitan area core: primary flow within an urbanized area");
    RUCA_DESCRIPTIONS.put(2, "Metropolitan area high commuting: primary flow 30%+ to UA");
    RUCA_DESCRIPTIONS.put(3, "Metropolitan area low commuting: primary flow 10-30% to UA");
    RUCA_DESCRIPTIONS.put(4, "Micropolitan area core: primary flow within urban cluster 10k+");
    RUCA_DESCRIPTIONS.put(5, "Micropolitan high commuting: primary flow 30%+ to large UC");
    RUCA_DESCRIPTIONS.put(6, "Micropolitan low commuting: primary flow 10-30% to large UC");
    RUCA_DESCRIPTIONS.put(7, "Small town core: primary flow within urban cluster 2,500-9,999");
    RUCA_DESCRIPTIONS.put(8, "Small town high commuting: primary flow 30%+ to small UC");
    RUCA_DESCRIPTIONS.put(9, "Small town low commuting: primary flow 10-30% to small UC");
    RUCA_DESCRIPTIONS.put(10, "Rural areas: primary flow to a tract outside UA or UC");
  }

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Empty response body for USDA RUCA data");
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
        int stateFipsIdx = findColumnIndex(headers, "STATE_FIPS", "STATEFP", "state_fips");
        int countyFipsIdx = findColumnIndex(headers, "COUNTY_FIPS", "COUNTYFP", "county_fips");
        int tractFipsIdx =
            findColumnIndex(headers, "TRACT_FIPS", "TRACTCE", "tract_fips", "GEOID", "STATE_COUNTY_TRACT_FIPS");
        int primaryRucaIdx =
            findColumnIndex(headers, "PRIMARY_RUCA_CODE_2020", "RUCA1", "primary_ruca", "RUCA_PRIMARY");
        int secondaryRucaIdx =
            findColumnIndex(headers, "SECONDARY_RUCA_CODE_2020", "RUCA2", "secondary_ruca", "RUCA_SECONDARY");
        int populationIdx = findColumnIndex(headers, "POPULATION", "POP", "TRACT_POP");

        String line;
        while ((line = reader.readLine()) != null) {
          if (line.trim().isEmpty()) {
            continue;
          }

          String[] values = parseCSVLine(line);
          ObjectNode record = JSON_MAPPER.createObjectNode();

          // Build tract FIPS - may be provided as full 11-digit or as components
          String tractFips = getValueSafe(values, tractFipsIdx);
          String stateFips = getValueSafe(values, stateFipsIdx);
          String countyFips = getValueSafe(values, countyFipsIdx);

          if (tractFips != null && tractFips.length() == 11) {
            // Full tract FIPS provided
            record.put("tract_fips", tractFips);
            record.put("state_fips", tractFips.substring(0, 2));
            record.put("county_fips", tractFips.substring(0, 5));
          } else if (stateFips != null && countyFips != null && tractFips != null) {
            // Build from components
            String fullFips = String.format("%02d%03d%s",
                Integer.parseInt(stateFips),
                Integer.parseInt(countyFips),
                tractFips);
            record.put("tract_fips", fullFips);
            record.put("state_fips", String.format("%02d", Integer.parseInt(stateFips)));
            record.put("county_fips",
                String.format("%02d%03d", Integer.parseInt(stateFips), Integer.parseInt(countyFips)));
          } else {
            continue; // Skip records without valid tract identification
          }

          String primaryStr = getValueSafe(values, primaryRucaIdx);
          if (primaryStr != null && !primaryStr.isEmpty()) {
            try {
              int primary = Integer.parseInt(primaryStr);
              record.put("ruca_primary", primary);
              record.put("ruca_description", RUCA_DESCRIPTIONS.get(primary));
            } catch (NumberFormatException e) {
              LOGGER.debug("Invalid primary RUCA code: {}", primaryStr);
            }
          }

          String secondaryStr = getValueSafe(values, secondaryRucaIdx);
          if (secondaryStr != null && !secondaryStr.isEmpty()) {
            try {
              record.put("ruca_secondary", Double.parseDouble(secondaryStr));
            } catch (NumberFormatException e) {
              LOGGER.debug("Invalid secondary RUCA code: {}", secondaryStr);
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

      LOGGER.info("Transformed {} RUCA records", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("Error transforming USDA RUCA data: {}", e.getMessage(), e);
      return "[]";
    }
  }

  private String[] parseCSVLine(String line) {
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
