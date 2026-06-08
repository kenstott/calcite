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
 * Transforms EPA AQS dailyData/byState API responses into a flat JSON array.
 *
 * <p>Input format:
 * <pre>{@code
 * {
 *   "Header": [{"status": "Success", "rows": 3089}],
 *   "Data": [
 *     {
 *       "state_code": "06", "county_code": "079", "site_number": "8002",
 *       "parameter_code": "88101", "parameter": "PM2.5 - Local Conditions",
 *       "date_local": "2025-01-01", "sample_duration": "24-HR BLK AVG",
 *       "pollutant_standard": "PM25 24-hour 2006",
 *       "arithmetic_mean": 11.1, "first_max_value": 11.1,
 *       "aqi": 55, "observation_count": 1,
 *       "latitude": 35.49, "longitude": -120.66
 *     }
 *   ]
 * }
 * }</pre>
 */
public class EpaDailyAqiTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(EpaDailyAqiTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EPA Daily AQI: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (!root.isObject()) {
        LOGGER.warn("EPA Daily AQI: Unexpected response type {} for {}",
            root.getNodeType(), context.getUrl());
        return "[]";
      }

      JsonNode header = root.get("Header");
      if (header != null && header.isArray() && header.size() > 0) {
        JsonNode status = header.get(0).get("status");
        if (status != null && !"Success".equalsIgnoreCase(status.asText())) {
          LOGGER.warn("EPA Daily AQI: API error '{}' for {}", status.asText(), context.getUrl());
          return "[]";
        }
      }

      JsonNode data = root.get("Data");
      if (data == null || !data.isArray()) {
        LOGGER.debug("EPA Daily AQI: No 'Data' array for {}", context.getUrl());
        return "[]";
      }

      String stateFips = context.getDimensionValues().get("state_fips");
      String paramCode = context.getDimensionValues().get("param");
      ArrayNode output = MAPPER.createArrayNode();

      for (JsonNode item : data) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("state_fips", stateFips);

        String stateCode = getTextOrNull(item, "state_code");
        String countyCode = getTextOrNull(item, "county_code");
        if (stateCode != null && countyCode != null) {
          row.put("county_fips", stateCode + countyCode);
        } else {
          row.putNull("county_fips");
        }

        row.put("site_number",
            paramCode != null ? getTextOrNull(item, "site_number") : null);
        row.put("parameter_code",
            paramCode != null ? paramCode : getTextOrNull(item, "parameter_code"));
        row.put("parameter_name", getTextOrNull(item, "parameter"));

        String dateLocal = getTextOrNull(item, "date_local");
        row.put("date", dateLocal);
        if (dateLocal != null && dateLocal.length() >= 4) {
          try {
            row.put("year", Integer.parseInt(dateLocal.substring(0, 4)));
          } catch (NumberFormatException e) {
            row.putNull("year");
          }
        } else {
          row.putNull("year");
        }

        row.put("sample_duration", getTextOrNull(item, "sample_duration"));
        row.put("pollutant_standard", getTextOrNull(item, "pollutant_standard"));
        putDoubleOrNull(row, "arithmetic_mean", item, "arithmetic_mean");
        putDoubleOrNull(row, "first_max_value", item, "first_max_value");
        putIntOrNull(row, "aqi", item, "aqi");
        putIntOrNull(row, "observation_count", item, "observation_count");
        putDoubleOrNull(row, "latitude", item, "latitude");
        putDoubleOrNull(row, "longitude", item, "longitude");

        output.add(row);
      }

      LOGGER.debug("EPA Daily AQI: Transformed {} records for state_fips={}, param={}",
          output.size(), stateFips, paramCode);
      return output.toString();

    } catch (Exception e) {
      LOGGER.error("EPA Daily AQI: Failed to parse response for {}: {}",
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

  private static void putDoubleOrNull(ObjectNode row, String key, JsonNode source, String field) {
    JsonNode value = source.get(field);
    if (value != null && value.isNumber()) {
      row.put(key, value.doubleValue());
    } else {
      row.putNull(key);
    }
  }

  private static void putIntOrNull(ObjectNode row, String key, JsonNode source, String field) {
    JsonNode value = source.get(field);
    if (value != null && value.isNumber()) {
      row.put(key, value.intValue());
    } else {
      row.putNull(key);
    }
  }
}
