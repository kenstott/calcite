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
 * Transforms FBI CDE police employment responses into flat rows.
 *
 * <p>The CDE /pe/{state} endpoint returns nested data keyed by year with
 * rates and actual counts for officers and civilians.
 *
 * <p>Output: One row per year with officers_per_1000, male/female officer
 * and civilian counts, and participated_population.
 */
public class CdePoliceEmploymentTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CdePoliceEmploymentTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDE PE: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      String stateAbbr = context.getDimensionValues().get("state_abbr");
      String yearDim = context.getDimensionValues().get("year");

      ArrayNode result = MAPPER.createArrayNode();

      if (root.isArray()) {
        // Direct array of records
        for (JsonNode item : root) {
          ObjectNode row = buildRow(item, stateAbbr);
          result.add(row);
        }
      } else if (root.isObject()) {
        // Check for "results" wrapper
        JsonNode results = root.get("results");
        if (results != null && results.isArray()) {
          for (JsonNode item : results) {
            ObjectNode row = buildRow(item, stateAbbr);
            result.add(row);
          }
        } else {
          // Interpret as year-keyed data
          Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
          while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();

            if ("pagination".equals(key) || "message".equals(key)) {
              continue;
            }

            if (value.isObject()) {
              ObjectNode row = MAPPER.createObjectNode();
              row.put("state_abbr", stateAbbr != null ? stateAbbr : "");

              // Try to parse key as year
              try {
                int year = Integer.parseInt(key);
                row.put("year", year);
              } catch (NumberFormatException e) {
                if (yearDim != null) {
                  row.put("year", Integer.parseInt(yearDim));
                }
              }

              extractEmploymentFields(row, value);
              result.add(row);
            }
          }
        }
      }

      LOGGER.debug("CDE PE: Transformed {} records for state={}",
          result.size(), stateAbbr);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("CDE PE: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private ObjectNode buildRow(JsonNode item, String stateAbbr) {
    ObjectNode row = MAPPER.createObjectNode();
    row.put("state_abbr", stateAbbr != null ? stateAbbr : "");

    JsonNode yearNode = item.get("data_year");
    if (yearNode == null) {
      yearNode = item.get("year");
    }
    if (yearNode != null) {
      row.put("year", yearNode.asInt());
    }

    extractEmploymentFields(row, item);
    return row;
  }

  private void extractEmploymentFields(ObjectNode row, JsonNode source) {
    putDoubleOrNull(row, "officers_per_1000", source,
        "pe_ct_per_1000", "officers_per_1000", "rate");
    putLongOrNull(row, "male_officers", source,
        "male_officer_ct", "male_officers", "male_total_officers");
    putLongOrNull(row, "female_officers", source,
        "female_officer_ct", "female_officers", "female_total_officers");
    putLongOrNull(row, "male_civilians", source,
        "male_civilian_ct", "male_civilians", "male_total_civilians");
    putLongOrNull(row, "female_civilians", source,
        "female_civilian_ct", "female_civilians", "female_total_civilians");
    putLongOrNull(row, "participated_population", source,
        "pe_ct_pop", "participated_population", "population");
  }

  private static void putDoubleOrNull(ObjectNode row, String key,
      JsonNode source, String... sourceKeys) {
    for (String sk : sourceKeys) {
      JsonNode value = source.get(sk);
      if (value != null && !value.isNull()) {
        if (value.isNumber()) {
          row.put(key, value.doubleValue());
          return;
        }
        try {
          row.put(key, Double.parseDouble(value.asText()));
          return;
        } catch (NumberFormatException e) {
          // continue
        }
      }
    }
    row.putNull(key);
  }

  private static void putLongOrNull(ObjectNode row, String key,
      JsonNode source, String... sourceKeys) {
    for (String sk : sourceKeys) {
      JsonNode value = source.get(sk);
      if (value != null && !value.isNull()) {
        if (value.isNumber()) {
          row.put(key, value.longValue());
          return;
        }
        try {
          row.put(key, Long.parseLong(value.asText()));
          return;
        } catch (NumberFormatException e) {
          // continue
        }
      }
    }
    row.putNull(key);
  }
}
