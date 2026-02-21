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
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms EPA AQS annual data API responses into a flat JSON array.
 *
 * <p>Input format:
 * <pre>{@code
 * {
 *   "Header": [{"status": "Success", "rows": 42}],
 *   "Data": [
 *     {
 *       "state_code": "06",
 *       "county_code": "037",
 *       "parameter_code": "88101",
 *       "parameter_name": "PM2.5 - Local Conditions",
 *       "year": 2020,
 *       "arithmetic_mean": 10.5,
 *       "first_max_value": 45.2,
 *       "observation_count": 350,
 *       "valid_day_count": 340,
 *       "aqi": 44
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>Enriches each record with {@code state_fips} from dimensions and builds
 * 5-digit {@code county_fips} from state_code + county_code.
 */
public class EpaAqsTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(EpaAqsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EPA AQS: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (!root.isObject()) {
        LOGGER.warn("EPA AQS: Unexpected response type {} for {}",
            root.getNodeType(), context.getUrl());
        return "[]";
      }

      // Check for error in Header
      JsonNode header = root.get("Header");
      if (header != null && header.isArray() && header.size() > 0) {
        JsonNode status = header.get(0).get("status");
        if (status != null && !"Success".equalsIgnoreCase(status.asText())) {
          LOGGER.warn("EPA AQS: API error status '{}' for {}",
              status.asText(), context.getUrl());
          return "[]";
        }
      }

      JsonNode data = root.get("Data");
      if (data == null || !data.isArray()) {
        LOGGER.debug("EPA AQS: No 'Data' array in response for {}", context.getUrl());
        return "[]";
      }

      String stateFips = context.getDimensionValues().get("state_fips");
      String paramCode = context.getDimensionValues().get("param");
      ArrayNode output = MAPPER.createArrayNode();

      for (JsonNode item : data) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("state_fips", stateFips);

        // Build 5-digit county FIPS from state_code + county_code
        String stateCode = getTextOrNull(item, "state_code");
        String countyCode = getTextOrNull(item, "county_code");
        if (stateCode != null && countyCode != null) {
          row.put("county_fips", stateCode + countyCode);
        } else {
          row.putNull("county_fips");
        }

        row.put("parameter_code",
            paramCode != null ? paramCode : getTextOrNull(item, "parameter_code"));
        // EPA API uses "parameter" not "parameter_name"
        String paramName = getTextOrNull(item, "parameter");
        if (paramName == null) {
          paramName = getTextOrNull(item, "parameter_name");
        }
        row.put("parameter_name", paramName);

        putIntOrNull(row, "year", item, "year");
        putDoubleOrNull(row, "arithmetic_mean", item, "arithmetic_mean");
        putDoubleOrNull(row, "first_max_value", item, "first_max_value");
        putIntOrNull(row, "observation_count", item, "observation_count");
        putIntOrNull(row, "valid_day_count", item, "valid_day_count");
        putIntOrNull(row, "aqi", item, "aqi");

        output.add(row);
      }

      LOGGER.debug("EPA AQS: Transformed {} records for state_fips={}, param={}",
          output.size(), stateFips, paramCode);
      return output.toString();

    } catch (Exception e) {
      LOGGER.error("EPA AQS: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
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

  private static void putIntOrNull(ObjectNode row, String key,
      JsonNode source, String field) {
    JsonNode value = source.get(field);
    if (value != null && value.isNumber()) {
      row.put(key, value.intValue());
    } else {
      row.putNull(key);
    }
  }
}
