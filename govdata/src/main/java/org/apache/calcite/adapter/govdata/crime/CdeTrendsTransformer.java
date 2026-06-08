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
 * Transforms FBI CDE national crime trends response into flat rows.
 *
 * <p>Input example:
 * <pre>{@code
 * {
 *   "crime_trends": {
 *     "cde_crime_trends": {
 *       "trends": {
 *         "Rape": "-7.5",
 *         "Murder": "-18.4",
 *         ...
 *       },
 *       "current_range": "November 2024 - October 2025",
 *       "last_refresh_date": "February 15, 2026"
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>Output: One row per offense with offense_name, trend_pct, current_range,
 * last_refresh_date.
 */
public class CdeTrendsTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CdeTrendsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDE Trends: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      JsonNode crimeTrends = root.path("crime_trends").path("cde_crime_trends");
      if (crimeTrends.isMissingNode()) {
        LOGGER.warn("CDE Trends: Missing crime_trends.cde_crime_trends in response");
        return "[]";
      }

      String currentRange = crimeTrends.path("current_range").asText("");
      String lastRefresh = crimeTrends.path("last_refresh_date").asText("");

      JsonNode trends = crimeTrends.path("trends");
      if (trends.isObject()) {
        Iterator<Map.Entry<String, JsonNode>> fields = trends.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> entry = fields.next();
          ObjectNode row = MAPPER.createObjectNode();
          row.put("offense_name", entry.getKey());

          String pctStr = entry.getValue().asText("");
          try {
            row.put("trend_pct", Double.parseDouble(pctStr));
          } catch (NumberFormatException e) {
            row.putNull("trend_pct");
          }

          row.put("current_range", currentRange);
          row.put("last_refresh_date", lastRefresh);
          result.add(row);
        }
      }

      LOGGER.debug("CDE Trends: Transformed {} trend records", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("CDE Trends: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }
}
