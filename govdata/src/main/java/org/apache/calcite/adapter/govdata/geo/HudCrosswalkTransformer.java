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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Transforms HUD USPS Crosswalk API responses to standard JSON array format.
 *
 * <p>HUD API responses come as JSON with a "data" array containing crosswalk records.
 * This transformer extracts and normalizes the records for Parquet conversion.
 */
public class HudCrosswalkTransformer implements ResponseTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(HudCrosswalkTransformer.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Empty response body for HUD crosswalk");
      return "[]";
    }

    try {
      JsonNode root = JSON_MAPPER.readTree(response);
      // Derive table name from URL since RequestContext doesn't have getTableName()
      String tableName = deriveTableNameFromUrl(context.getUrl());
      LOGGER.info("Transforming HUD crosswalk for table: {}", tableName);

      // HUD API returns { "data": [ ... ] }
      JsonNode data = root.get("data");
      if (data == null || !data.isArray()) {
        // Try root as array
        if (root.isArray()) {
          data = root;
        } else {
          LOGGER.warn("No 'data' array found in HUD response for {}", tableName);
          return "[]";
        }
      }

      // Transform based on table type
      ArrayNode result = JSON_MAPPER.createArrayNode();
      for (JsonNode record : data) {
        ObjectNode transformed = transformRecord(record, tableName);
        if (transformed != null) {
          result.add(transformed);
        }
      }

      LOGGER.info("Transformed {} records from HUD crosswalk for table {}",
          result.size(), tableName);

      return result.toString();

    } catch (Exception e) {
      LOGGER.error("Error transforming HUD crosswalk: {}", e.getMessage(), e);
      return "[]";
    }
  }

  /**
   * Derives table name from HUD API URL.
   * HUD URLs have format: https://www.huduser.gov/hudapi/public/usps?type=N
   * where:
   *   type=1: ZIP to Tract
   *   type=2: ZIP to County (legacy, use type=1 style URL param)
   *   type=3: Tract to ZIP (reverse)
   *   type=4: ZIP to CBSA
   *   type=5: ZIP to Congressional District
   *   type=7: County to ZIP (reverse)
   *   type=10: Congressional District to ZIP (reverse)
   */
  private String deriveTableNameFromUrl(String url) {
    if (url == null) {
      return "unknown";
    }
    // Check for specific type parameter - order matters for substring matching
    if (url.contains("type=10")) {
      return "cd_zip_crosswalk";
    } else if (url.contains("type=1")) {
      // type=1 is ZIP to Tract (also handles legacy zip_county if type=1 was used)
      return "zip_tract_crosswalk";
    } else if (url.contains("type=2")) {
      return "zip_county_crosswalk";
    } else if (url.contains("type=3")) {
      return "tract_zip_crosswalk";
    } else if (url.contains("type=4")) {
      return "zip_cbsa_crosswalk";
    } else if (url.contains("type=5")) {
      return "zip_cd_crosswalk";
    } else if (url.contains("type=7")) {
      return "county_zip_crosswalk";
    }
    return "unknown";
  }

  private ObjectNode transformRecord(JsonNode record, String tableName) {
    ObjectNode result = JSON_MAPPER.createObjectNode();

    switch (tableName) {
    case "zip_county_crosswalk":
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("county_fips", getStringField(record, "county", "COUNTY", "geoid"));
      result.put("state_fips", getStringField(record, "state", "STATE", "usps_zip_pref_state"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "zip_cbsa_crosswalk":
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("cbsa_code", getStringField(record, "cbsa", "CBSA", "geoid"));
      result.put("cbsa_name", getStringField(record, "cbsa_name", "usps_zip_pref_city"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "tract_zip_crosswalk":
      result.put("tract_fips", getStringField(record, "tract", "TRACT", "geoid"));
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "zip_tract_crosswalk":
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("tract_fips", getStringField(record, "tract", "TRACT", "geoid"));
      result.put("state_fips", getStringField(record, "state", "STATE", "usps_zip_pref_state"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "zip_cd_crosswalk":
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("cd_fips", getStringField(record, "cd", "CD", "geoid", "congressional_district"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "county_zip_crosswalk":
      result.put("county_fips", getStringField(record, "county", "COUNTY", "geoid"));
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "cd_zip_crosswalk":
      result.put("cd_fips", getStringField(record, "cd", "CD", "geoid", "congressional_district"));
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    default:
      // Generic - copy all fields
      Iterator<String> fieldNames = record.fieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        JsonNode value = record.get(fieldName);
        if (value.isTextual()) {
          result.put(fieldName.toLowerCase(), value.asText());
        } else if (value.isNumber()) {
          result.put(fieldName.toLowerCase(), value.doubleValue());
        }
      }
    }

    return result;
  }

  private String getStringField(JsonNode record, String... keys) {
    for (String key : keys) {
      JsonNode node = record.get(key);
      if (node != null && !node.isNull()) {
        return node.asText();
      }
    }
    return null;
  }

  private Double getDoubleField(JsonNode record, String... keys) {
    for (String key : keys) {
      JsonNode node = record.get(key);
      if (node != null && !node.isNull()) {
        if (node.isNumber()) {
          return node.doubleValue();
        }
        try {
          return Double.parseDouble(node.asText());
        } catch (NumberFormatException e) {
          // Continue to next key
        }
      }
    }
    return null;
  }
}
