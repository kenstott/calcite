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
 * Transforms FBI CDE supplemental property crime response into flat rows.
 *
 * <p>The CDE /supplemental/state/{st}/property-crime?type=totals endpoint returns
 * stolen and recovered property values by category.
 *
 * <p>Input example:
 * <pre>{@code
 * {
 *   "stolen_and_recovered": {
 *     "stolen_value": {
 *       "Firearms": 34309810,
 *       "Motor Vehicles": 901098938,
 *       ...
 *     },
 *     "recovered_value": {
 *       "Firearms": 449433,
 *       ...
 *     }
 *   },
 *   "offense_analysis": { ... }
 * }
 * }</pre>
 *
 * <p>Output: One row per property category with state_abbr, year,
 * property_type, stolen_value, recovered_value.
 */
public class CdeSupplementalTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CdeSupplementalTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDE Supplemental: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      String stateAbbr = context.getDimensionValues().get("state_abbr");
      String yearDim = context.getDimensionValues().get("year");
      int year = 0;
      if (yearDim != null) {
        try {
          year = Integer.parseInt(yearDim);
        } catch (NumberFormatException e) {
          // leave as 0
        }
      }

      ArrayNode result = MAPPER.createArrayNode();

      // Extract stolen_and_recovered section
      JsonNode stolenAndRecovered = root.path("stolen_and_recovered");
      if (!stolenAndRecovered.isMissingNode() && stolenAndRecovered.isObject()) {
        JsonNode stolenValues = stolenAndRecovered.path("stolen_value");
        JsonNode recoveredValues = stolenAndRecovered.path("recovered_value");

        if (stolenValues.isObject()) {
          Iterator<Map.Entry<String, JsonNode>> fields = stolenValues.fields();
          while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String propertyType = entry.getKey();

            ObjectNode row = MAPPER.createObjectNode();
            row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
            if (year > 0) {
              row.put("year", year);
            }
            row.put("property_type", propertyType);

            // Stolen value
            JsonNode stolenNode = entry.getValue();
            if (stolenNode.isNumber()) {
              row.put("stolen_value", stolenNode.longValue());
            } else {
              row.putNull("stolen_value");
            }

            // Recovered value (matched by property_type key)
            JsonNode recoveredNode = recoveredValues.path(propertyType);
            if (recoveredNode.isNumber()) {
              row.put("recovered_value", recoveredNode.longValue());
            } else {
              row.putNull("recovered_value");
            }

            result.add(row);
          }
        }
      }

      LOGGER.debug("CDE Supplemental: Transformed {} records for state={}, year={}",
          result.size(), stateAbbr, year);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("CDE Supplemental: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }
}
