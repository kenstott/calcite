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
 * Transforms NOAA CDO v2 data API responses (GSOM/GSOY) into flat JSON arrays.
 *
 * <p>Reused for both monthly (GSOM) and annual (GSOY) summary tables.
 *
 * <p>Input format:
 * <pre>{@code
 * {
 *   "metadata": {"resultset": {"offset": 1, "count": 25, "limit": 1000}},
 *   "results": [
 *     {
 *       "date": "2020-01-01T00:00:00",
 *       "datatype": "TAVG",
 *       "station": "GHCND:USW00094846",
 *       "attributes": "H,,S,",
 *       "value": -2.3
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>Enriches each record with {@code state_fips} from dimensions and extracts
 * {@code year} and {@code month} from the date field.
 */
public class CdoDataTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CdoDataTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDO Data: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // CDO returns empty object {} or error message when no data available
      if (!root.isObject()) {
        LOGGER.warn("CDO Data: Unexpected response type {} for {}",
            root.getNodeType(), context.getUrl());
        return "[]";
      }

      JsonNode results = root.get("results");
      if (results == null || !results.isArray()) {
        // No data for this state/year combination — not an error
        LOGGER.debug("CDO Data: No results in response for {}", context.getUrl());
        return "[]";
      }

      String stateFips = context.getDimensionValues().get("state_fips");
      ArrayNode output = MAPPER.createArrayNode();

      for (JsonNode item : results) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("state_fips", stateFips);
        row.put("station_id", getTextOrNull(item, "station"));

        // Parse date to extract year and month
        String date = getTextOrNull(item, "date");
        row.put("date", date);
        if (date != null && date.length() >= 7) {
          try {
            row.put("year", Integer.parseInt(date.substring(0, 4)));
            row.put("month", Integer.parseInt(date.substring(5, 7)));
          } catch (NumberFormatException e) {
            row.putNull("year");
            row.putNull("month");
          }
        } else {
          row.putNull("year");
          row.putNull("month");
        }

        row.put("datatype", getTextOrNull(item, "datatype"));

        JsonNode value = item.get("value");
        if (value != null && value.isNumber()) {
          row.put("value", value.doubleValue());
        } else {
          row.putNull("value");
        }

        row.put("attributes", getTextOrNull(item, "attributes"));

        output.add(row);
      }

      LOGGER.debug("CDO Data: Transformed {} records for state_fips={}",
          output.size(), stateFips);
      return output.toString();

    } catch (Exception e) {
      LOGGER.error("CDO Data: Failed to parse response for {}: {}",
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
}
