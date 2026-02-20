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
 * Transforms FBI CDE summarized crime rate responses into flat rows.
 *
 * <p>The CDE /summarized/state/{state}/{offense} endpoint returns a nested structure
 * with rates, clearances, and populations keyed by "MM-YYYY" month strings.
 * State-level and national-level data appear as separate keys within the response.
 *
 * <p>Output: One row per month with state_abbr, offense_code, month, offense_rate,
 * clearance_rate, national_offense_rate, national_clearance_rate, population,
 * and population_coverage_pct.
 */
public class CdeCrimeRatesTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CdeCrimeRatesTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDE Crime: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // The response may be an object with nested rate/clearance/population data,
      // or it may be a direct array of results
      if (root.isArray()) {
        return transformArrayResponse(root, context);
      }

      if (!root.isObject()) {
        LOGGER.warn("CDE Crime: Unexpected response type {} for {}",
            root.getNodeType(), context.getUrl());
        return "[]";
      }

      return transformObjectResponse(root, context);

    } catch (Exception e) {
      LOGGER.error("CDE Crime: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private String transformArrayResponse(JsonNode root, RequestContext context) {
    // If the API returns a direct array, pass through with dimension enrichment
    String stateAbbr = context.getDimensionValues().get("state_abbr");
    String offenseCode = context.getDimensionValues().get("offense");

    ArrayNode result = MAPPER.createArrayNode();
    for (JsonNode item : root) {
      ObjectNode row = MAPPER.createObjectNode();
      row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
      row.put("offense_code", offenseCode != null ? offenseCode : "");

      // Copy all fields from the item
      Iterator<Map.Entry<String, JsonNode>> fields = item.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        row.set(field.getKey(), field.getValue());
      }
      result.add(row);
    }

    LOGGER.debug("CDE Crime: Transformed {} array records for state={}, offense={}",
        result.size(), stateAbbr, offenseCode);
    return result.toString();
  }

  private String transformObjectResponse(JsonNode root, RequestContext context) {
    String stateAbbr = context.getDimensionValues().get("state_abbr");
    String offenseCode = context.getDimensionValues().get("offense");

    ArrayNode result = MAPPER.createArrayNode();

    // Extract rate data sections
    // The CDE API typically returns structured data with keys like:
    // "results" containing arrays, or nested month-keyed data
    JsonNode results = root.get("results");
    if (results != null && results.isArray()) {
      for (JsonNode item : results) {
        ObjectNode row = buildRow(item, stateAbbr, offenseCode);
        result.add(row);
      }
    } else {
      // Try to interpret as month-keyed data
      Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        String key = entry.getKey();
        JsonNode value = entry.getValue();

        // Skip metadata fields
        if ("pagination".equals(key) || "message".equals(key)) {
          continue;
        }

        if (value.isObject()) {
          ObjectNode row = MAPPER.createObjectNode();
          row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
          row.put("offense_code", offenseCode != null ? offenseCode : "");
          row.put("month", key);
          copyNumericFields(row, value);
          result.add(row);
        } else if (value.isArray()) {
          // Handle array of records within a key
          for (JsonNode item : value) {
            ObjectNode row = buildRow(item, stateAbbr, offenseCode);
            result.add(row);
          }
        }
      }
    }

    LOGGER.debug("CDE Crime: Transformed {} records for state={}, offense={}",
        result.size(), stateAbbr, offenseCode);
    return result.toString();
  }

  private ObjectNode buildRow(JsonNode item, String stateAbbr, String offenseCode) {
    ObjectNode row = MAPPER.createObjectNode();
    row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
    row.put("offense_code", offenseCode != null ? offenseCode : "");

    // Map known fields
    putStringField(row, "month", item, "date", "month");
    putDoubleField(row, "offense_rate", item, "rate", "offense_rate", "actual_rate");
    putDoubleField(row, "clearance_rate", item, "cleared_rate", "clearance_rate");
    putDoubleField(row, "national_offense_rate", item,
        "us_rate", "national_rate", "national_offense_rate");
    putDoubleField(row, "national_clearance_rate", item,
        "us_cleared_rate", "national_clearance_rate");
    putLongField(row, "population", item, "population");
    putDoubleField(row, "population_coverage_pct", item,
        "ori_coverage_pct", "population_coverage_pct", "coverage_pct");

    return row;
  }

  private static void copyNumericFields(ObjectNode row, JsonNode source) {
    Iterator<Map.Entry<String, JsonNode>> fields = source.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      JsonNode value = field.getValue();
      if (value.isNumber()) {
        row.put(field.getKey(), value.doubleValue());
      } else if (value.isTextual()) {
        row.put(field.getKey(), value.asText());
      }
    }
  }

  private static void putStringField(ObjectNode row, String targetKey,
      JsonNode source, String... sourceKeys) {
    for (String key : sourceKeys) {
      JsonNode value = source.get(key);
      if (value != null && !value.isNull() && value.isTextual()) {
        row.put(targetKey, value.asText());
        return;
      }
    }
  }

  private static void putDoubleField(ObjectNode row, String targetKey,
      JsonNode source, String... sourceKeys) {
    for (String key : sourceKeys) {
      JsonNode value = source.get(key);
      if (value != null && !value.isNull()) {
        if (value.isNumber()) {
          row.put(targetKey, value.doubleValue());
          return;
        }
        try {
          row.put(targetKey, Double.parseDouble(value.asText()));
          return;
        } catch (NumberFormatException e) {
          // continue to next key
        }
      }
    }
  }

  private static void putLongField(ObjectNode row, String targetKey,
      JsonNode source, String... sourceKeys) {
    for (String key : sourceKeys) {
      JsonNode value = source.get(key);
      if (value != null && !value.isNull()) {
        if (value.isNumber()) {
          row.put(targetKey, value.longValue());
          return;
        }
        try {
          row.put(targetKey, Long.parseLong(value.asText()));
          return;
        } catch (NumberFormatException e) {
          // continue to next key
        }
      }
    }
  }
}
