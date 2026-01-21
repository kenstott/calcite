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

      // HUD API returns { "data": { "results": [ ... ] } }
      JsonNode data = root.get("data");
      if (data == null) {
        LOGGER.warn("No 'data' field found in HUD response for {}", tableName);
        return "[]";
      }

      // Check if data has a "results" array (newer API format)
      JsonNode results = data.get("results");
      if (results != null && results.isArray()) {
        data = results;
      } else if (!data.isArray()) {
        // Try root as array for backwards compatibility
        if (root.isArray()) {
          data = root;
        } else {
          LOGGER.warn("No 'results' array found in HUD response for {}", tableName);
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
   *   type=1: ZIP to Tract (zip-tract)
   *   type=2: ZIP to County (zip-county)
   *   type=3: ZIP to CBSA (zip-cbsa)
   *   type=4: ZIP to CBSA Division (zip-cbsadiv)
   *   type=5: ZIP to Congressional District (zip-cd)
   *   type=6: Tract to ZIP (tract-zip) [REVERSE]
   *   type=7: County to ZIP (county-zip) [REVERSE]
   *   type=8: CBSA to ZIP (cbsa-zip) [REVERSE]
   *   type=9: CBSA Division to ZIP (cbsadiv-zip) [REVERSE]
   *   type=10: Congressional District to ZIP (cd-zip) [REVERSE]
   */
  private String deriveTableNameFromUrl(String url) {
    if (url == null) {
      return "unknown";
    }
    // Check for specific type parameter - order matters for substring matching
    // Multi-digit types must be checked first to avoid partial matches
    if (url.contains("type=10")) {
      return "cd_zip_crosswalk";
    } else if (url.contains("type=1&") || url.endsWith("type=1")) {
      return "zip_tract_crosswalk";
    } else if (url.contains("type=2&") || url.endsWith("type=2")) {
      return "zip_county_crosswalk";
    } else if (url.contains("type=3&") || url.endsWith("type=3")) {
      return "zip_cbsa_crosswalk";
    } else if (url.contains("type=5&") || url.endsWith("type=5")) {
      return "zip_cd_crosswalk";
    } else if (url.contains("type=6&") || url.endsWith("type=6")) {
      return "tract_zip_crosswalk";
    } else if (url.contains("type=7&") || url.endsWith("type=7")) {
      return "county_zip_crosswalk";
    }
    return "unknown";
  }

  private ObjectNode transformRecord(JsonNode record, String tableName) {
    ObjectNode result = JSON_MAPPER.createObjectNode();

    // HUD API uses "geoid" for the target geography in forward lookups (ZIP to X)
    // and for the ZIP in reverse lookups (X to ZIP)
    switch (tableName) {
    case "zip_county_crosswalk":
      // type=2: ZIP to County - geoid contains county FIPS
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("county_fips", getStringField(record, "geoid", "county", "COUNTY"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "zip_cbsa_crosswalk":
      // type=3: ZIP to CBSA - geoid contains CBSA code
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("cbsa_code", getStringField(record, "geoid", "cbsa", "CBSA"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "tract_zip_crosswalk":
      // type=6: Tract to ZIP - geoid contains ZIP, tract field has tract FIPS
      result.put("tract_fips", getStringField(record, "tract", "TRACT"));
      result.put("zip", getStringField(record, "geoid", "zip", "ZIP"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "zip_tract_crosswalk":
      // type=1: ZIP to Tract - geoid contains tract FIPS
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("tract_fips", getStringField(record, "geoid", "tract", "TRACT"));
      // Extract state_fips from tract_fips (first 2 digits)
      String tractFips = getStringField(record, "geoid", "tract", "TRACT");
      if (tractFips != null && tractFips.length() >= 2) {
        result.put("state_fips", tractFips.substring(0, 2));
      }
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "zip_cd_crosswalk":
      // type=5: ZIP to CD - geoid contains CD code
      result.put("zip", getStringField(record, "zip", "ZIP"));
      result.put("cd_fips", getStringField(record, "geoid", "cd", "CD"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "county_zip_crosswalk":
      // type=7: County to ZIP - geoid contains ZIP, county field has county FIPS
      result.put("county_fips", getStringField(record, "county", "COUNTY"));
      result.put("zip", getStringField(record, "geoid", "zip", "ZIP"));
      result.put("res_ratio", getDoubleField(record, "res_ratio", "RES_RATIO"));
      result.put("bus_ratio", getDoubleField(record, "bus_ratio", "BUS_RATIO"));
      result.put("oth_ratio", getDoubleField(record, "oth_ratio", "OTH_RATIO"));
      result.put("tot_ratio", getDoubleField(record, "tot_ratio", "TOT_RATIO"));
      break;

    case "cd_zip_crosswalk":
      // type=10: CD to ZIP - geoid contains ZIP, cd field has CD code
      result.put("cd_fips", getStringField(record, "cd", "CD"));
      result.put("zip", getStringField(record, "geoid", "zip", "ZIP"));
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
