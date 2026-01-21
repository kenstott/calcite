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

/**
 * Transforms Census Bureau Gazetteer files (tab-separated) to JSON format.
 *
 * <p>Gazetteer files contain authoritative names, codes, area measurements, and
 * representative coordinates for geographic entities. Files are tab-delimited.
 *
 * <p>Common columns across gazetteer types:
 * <ul>
 *   <li>GEOID - Geographic identifier (FIPS code)</li>
 *   <li>NAME - Geographic entity name</li>
 *   <li>ALAND - Land area in square meters</li>
 *   <li>AWATER - Water area in square meters</li>
 *   <li>ALAND_SQMI - Land area in square miles</li>
 *   <li>AWATER_SQMI - Water area in square miles</li>
 *   <li>INTPTLAT - Internal point latitude</li>
 *   <li>INTPTLONG - Internal point longitude</li>
 * </ul>
 */
public class GazetteerTransformer implements ResponseTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(GazetteerTransformer.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Empty response body for Gazetteer data");
      return "[]";
    }

    try {
      ArrayNode result = JSON_MAPPER.createArrayNode();
      String tableName = deriveTableNameFromUrl(context.getUrl());

      try (BufferedReader reader = new BufferedReader(new StringReader(response))) {
        String headerLine = reader.readLine();
        if (headerLine == null) {
          return "[]";
        }

        // Parse tab-delimited header
        String[] headers = headerLine.split("\t");
        for (int i = 0; i < headers.length; i++) {
          headers[i] = headers[i].trim();
        }

        String line;
        while ((line = reader.readLine()) != null) {
          if (line.trim().isEmpty()) {
            continue;
          }

          String[] values = line.split("\t", -1);
          ObjectNode record = transformRecord(headers, values, tableName);
          if (record != null) {
            result.add(record);
          }
        }
      }

      LOGGER.info("Transformed {} Gazetteer records for {}", result.size(), tableName);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("Error transforming Gazetteer data: {}", e.getMessage(), e);
      return "[]";
    }
  }

  private String deriveTableNameFromUrl(String url) {
    if (url == null) {
      return "unknown";
    }
    if (url.contains("counties") || url.contains("county")) {
      return "gazetteer_counties";
    } else if (url.contains("place")) {
      return "gazetteer_places";
    } else if (url.contains("zcta")) {
      return "gazetteer_zctas";
    } else if (url.contains("tract")) {
      return "gazetteer_tracts";
    }
    return "unknown";
  }

  private ObjectNode transformRecord(String[] headers, String[] values, String tableName) {
    ObjectNode record = JSON_MAPPER.createObjectNode();

    switch (tableName) {
    case "gazetteer_counties":
      record.put("county_fips", getStringValue(headers, values, "GEOID"));
      String countyGeoid = getStringValue(headers, values, "GEOID");
      if (countyGeoid != null && countyGeoid.length() >= 2) {
        record.put("state_fips", countyGeoid.substring(0, 2));
      }
      record.put("county_name", getStringValue(headers, values, "NAME"));
      record.put("land_area_sqmi", getDoubleValue(headers, values, "ALAND_SQMI", "ALAND"));
      record.put("water_area_sqmi", getDoubleValue(headers, values, "AWATER_SQMI", "AWATER"));
      record.put("latitude", getDoubleValue(headers, values, "INTPTLAT"));
      record.put("longitude", getDoubleValue(headers, values, "INTPTLONG"));
      break;

    case "gazetteer_places":
      record.put("place_fips", getStringValue(headers, values, "GEOID"));
      String placeGeoid = getStringValue(headers, values, "GEOID");
      if (placeGeoid != null && placeGeoid.length() >= 2) {
        record.put("state_fips", placeGeoid.substring(0, 2));
      }
      record.put("place_name", getStringValue(headers, values, "NAME"));
      record.put("place_type", getStringValue(headers, values, "LSAD", "CLASSFP"));
      record.put("land_area_sqmi", getDoubleValue(headers, values, "ALAND_SQMI", "ALAND"));
      record.put("water_area_sqmi", getDoubleValue(headers, values, "AWATER_SQMI", "AWATER"));
      record.put("latitude", getDoubleValue(headers, values, "INTPTLAT"));
      record.put("longitude", getDoubleValue(headers, values, "INTPTLONG"));
      Integer placePop = getIntValue(headers, values, "POP", "POPULATION", "POP10");
      if (placePop != null) {
        record.put("population", placePop);
      }
      Integer placeHu = getIntValue(headers, values, "HU", "HOUSING", "HU10");
      if (placeHu != null) {
        record.put("housing_units", placeHu);
      }
      break;

    case "gazetteer_zctas":
      record.put("zcta", getStringValue(headers, values, "GEOID", "ZCTA5"));
      record.put("land_area_sqmi", getDoubleValue(headers, values, "ALAND_SQMI", "ALAND"));
      record.put("water_area_sqmi", getDoubleValue(headers, values, "AWATER_SQMI", "AWATER"));
      record.put("latitude", getDoubleValue(headers, values, "INTPTLAT"));
      record.put("longitude", getDoubleValue(headers, values, "INTPTLONG"));
      Integer zctaPop = getIntValue(headers, values, "POP", "POPULATION", "POP10");
      if (zctaPop != null) {
        record.put("population", zctaPop);
      }
      Integer zctaHu = getIntValue(headers, values, "HU", "HOUSING", "HU10");
      if (zctaHu != null) {
        record.put("housing_units", zctaHu);
      }
      break;

    default:
      // Generic - copy all columns with lowercase names
      for (int i = 0; i < headers.length && i < values.length; i++) {
        String value = values[i].trim();
        if (!value.isEmpty()) {
          record.put(headers[i].toLowerCase(), value);
        }
      }
    }

    return record;
  }

  private int findColumnIndex(String[] headers, String... names) {
    for (String name : names) {
      for (int i = 0; i < headers.length; i++) {
        if (headers[i].equalsIgnoreCase(name)) {
          return i;
        }
      }
    }
    return -1;
  }

  private String getStringValue(String[] headers, String[] values, String... columnNames) {
    int idx = findColumnIndex(headers, columnNames);
    if (idx >= 0 && idx < values.length) {
      String value = values[idx].trim();
      return value.isEmpty() ? null : value;
    }
    return null;
  }

  private Double getDoubleValue(String[] headers, String[] values, String... columnNames) {
    String strValue = getStringValue(headers, values, columnNames);
    if (strValue != null) {
      try {
        // Handle land/water area conversion if in square meters
        double value = Double.parseDouble(strValue);
        // If column is ALAND/AWATER (square meters), convert to square miles
        int idx = findColumnIndex(headers, columnNames);
        if (idx >= 0 && (headers[idx].equals("ALAND") || headers[idx].equals("AWATER"))) {
          return value / 2589988.11; // square meters to square miles
        }
        return value;
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private Integer getIntValue(String[] headers, String[] values, String... columnNames) {
    String strValue = getStringValue(headers, values, columnNames);
    if (strValue != null) {
      try {
        return Integer.parseInt(strValue.replace(",", ""));
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
