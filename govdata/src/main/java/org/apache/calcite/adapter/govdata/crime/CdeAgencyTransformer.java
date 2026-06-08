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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Transforms FBI CDE agency-by-state responses into flat rows.
 *
 * <p>Input format: {@code {"KERN": [{agency}, ...], "LOS ANGELES": [{agency}, ...]}}
 * <p>Output: Flat JSON array with {@code county_name} injected from the dictionary key,
 * normalized to title case.
 */
public class CdeAgencyTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CdeAgencyTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDE Agency: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (!root.isObject()) {
        LOGGER.warn("CDE Agency: Expected object response, got {}", root.getNodeType());
        return "[]";
      }

      ArrayNode result = MAPPER.createArrayNode();
      String stateAbbr = context.getDimensionValues().get("state_abbr");

      Iterator<Map.Entry<String, JsonNode>> counties = root.fields();
      while (counties.hasNext()) {
        Map.Entry<String, JsonNode> entry = counties.next();
        String countyName = toTitleCase(entry.getKey());
        JsonNode agencies = entry.getValue();

        if (!agencies.isArray()) {
          continue;
        }

        for (JsonNode agency : agencies) {
          ObjectNode row = MAPPER.createObjectNode();
          row.put("ori", getTextOrNull(agency, "ori"));
          row.put("agency_name", getTextOrNull(agency, "agency_name"));
          row.put("agency_type_name", getTextOrNull(agency, "agency_type_name"));
          row.put("state_abbr", stateAbbr != null ? stateAbbr
              : getTextOrNull(agency, "state_abbr"));
          row.put("county_name", countyName);

          // NIBRS fields
          JsonNode nibrs = agency.get("is_nibrs");
          if (nibrs != null && nibrs.isBoolean()) {
            row.put("is_nibrs", nibrs.booleanValue());
          } else {
            row.putNull("is_nibrs");
          }
          row.put("nibrs_start_date", getTextOrNull(agency, "nibrs_start_date"));

          // Coordinates
          putDoubleOrNull(row, "latitude", agency, "latitude");
          putDoubleOrNull(row, "longitude", agency, "longitude");

          result.add(row);
        }
      }

      LOGGER.debug("CDE Agency: Transformed {} agencies for state={}",
          result.size(), stateAbbr);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("CDE Agency: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  /**
   * Converts an UPPERCASE string to Title Case.
   * E.g., "LOS ANGELES" becomes "Los Angeles".
   */
  static String toTitleCase(String input) {
    if (input == null || input.isEmpty()) {
      return input;
    }
    StringBuilder sb = new StringBuilder();
    boolean capitalizeNext = true;
    for (char c : input.toCharArray()) {
      if (Character.isWhitespace(c) || c == '-') {
        capitalizeNext = true;
        sb.append(c);
      } else if (capitalizeNext) {
        sb.append(Character.toUpperCase(c));
        capitalizeNext = false;
      } else {
        sb.append(Character.toLowerCase(c));
      }
    }
    return sb.toString();
  }

  private static String getTextOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asText();
  }

  private static void putDoubleOrNull(ObjectNode row, String key,
      JsonNode source, String field) {
    JsonNode value = source.get(field);
    if (value != null && value.isNumber()) {
      row.put(key, value.doubleValue());
    } else {
      row.putNull(key);
    }
  }
}
