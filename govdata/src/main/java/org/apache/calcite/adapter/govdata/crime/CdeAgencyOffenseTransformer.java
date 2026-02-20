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
 * Transforms FBI CDE agency-level offense responses into flat rows.
 *
 * <p>The CDE /summarized/agency/{ori}/{offense} endpoint returns agency-level
 * crime data including actual counts and clearances. This transformer flattens
 * the response and enriches with ori, state_abbr, and offense_code from
 * dimension context.
 */
public class CdeAgencyOffenseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CdeAgencyOffenseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDE AgencyOffense: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      String ori = context.getDimensionValues().get("ori");
      String stateAbbr = context.getDimensionValues().get("state_abbr");
      String offenseCode = context.getDimensionValues().get("offense");

      ArrayNode result = MAPPER.createArrayNode();

      if (root.isArray()) {
        for (JsonNode item : root) {
          ObjectNode row = buildRow(item, ori, stateAbbr, offenseCode);
          result.add(row);
        }
      } else if (root.isObject()) {
        // Check for "results" wrapper
        JsonNode results = root.get("results");
        if (results != null && results.isArray()) {
          for (JsonNode item : results) {
            ObjectNode row = buildRow(item, ori, stateAbbr, offenseCode);
            result.add(row);
          }
        } else {
          // Try to interpret as month-keyed or year-keyed data
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
              row.put("ori", ori != null ? ori : "");
              row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
              row.put("offense_code", offenseCode != null ? offenseCode : "");
              row.put("month", key);
              copyFields(row, value);
              result.add(row);
            } else if (value.isArray()) {
              for (JsonNode item : value) {
                ObjectNode row = buildRow(item, ori, stateAbbr, offenseCode);
                result.add(row);
              }
            }
          }
        }
      }

      LOGGER.debug("CDE AgencyOffense: Transformed {} records for ori={}, offense={}",
          result.size(), ori, offenseCode);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("CDE AgencyOffense: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private ObjectNode buildRow(JsonNode item, String ori, String stateAbbr,
      String offenseCode) {
    ObjectNode row = MAPPER.createObjectNode();
    row.put("ori", ori != null ? ori : "");
    row.put("state_abbr", stateAbbr != null ? stateAbbr : "");
    row.put("offense_code", offenseCode != null ? offenseCode : "");

    // Map common fields
    putStringField(row, "month", item, "date", "month");
    putLongField(row, "actual", item, "actual", "offense_count");
    putLongField(row, "cleared", item, "cleared", "clearance_count");
    putLongField(row, "actual_since_last_year", item,
        "actual_since_last_year", "ytd_actual");
    putLongField(row, "cleared_since_last_year", item,
        "cleared_since_last_year", "ytd_cleared");

    return row;
  }

  private static void copyFields(ObjectNode row, JsonNode source) {
    Iterator<Map.Entry<String, JsonNode>> fields = source.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      JsonNode value = field.getValue();
      if (value.isNumber()) {
        row.put(field.getKey(), value.longValue());
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
          // continue
        }
      }
    }
  }
}
