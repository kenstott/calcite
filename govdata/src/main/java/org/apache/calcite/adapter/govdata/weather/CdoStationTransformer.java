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
 * Transforms NOAA CDO v2 station API responses into a flat JSON array.
 *
 * <p>Input format:
 * <pre>{@code
 * {
 *   "metadata": {"resultset": {"offset": 1, "count": 500, "limit": 1000}},
 *   "results": [
 *     {
 *       "id": "GHCND:USW00094846",  // or "COOP:110050", etc.
 *       "name": "CHICAGO OHARE INTERNATIONAL AIRPORT, IL US",
 *       "latitude": 41.9606,
 *       "longitude": -87.9317,
 *       "elevation": 201.8,
 *       "mindate": "1958-11-01",
 *       "maxdate": "2024-12-31",
 *       "datacoverage": 1.0
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>Enriches each station record with {@code state_fips} from dimensions.
 */
public class CdoStationTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CdoStationTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CDO Station: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (!root.isObject()) {
        LOGGER.warn("CDO Station: Unexpected response type {} for {}",
            root.getNodeType(), context.getUrl());
        return "[]";
      }

      JsonNode results = root.get("results");
      if (results == null || !results.isArray()) {
        LOGGER.debug("CDO Station: No results in response for {}", context.getUrl());
        return "[]";
      }

      String stateFips = context.getDimensionValues().get("state_fips");
      ArrayNode output = MAPPER.createArrayNode();

      for (JsonNode item : results) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("station_id", getTextOrNull(item, "id"));
        row.put("station_name", getTextOrNull(item, "name"));
        row.put("state_fips", stateFips);

        putDoubleOrNull(row, "latitude", item, "latitude");
        putDoubleOrNull(row, "longitude", item, "longitude");
        putDoubleOrNull(row, "elevation", item, "elevation");

        row.put("min_date", getTextOrNull(item, "mindate"));
        row.put("max_date", getTextOrNull(item, "maxdate"));

        putDoubleOrNull(row, "datacoverage", item, "datacoverage");

        output.add(row);
      }

      LOGGER.debug("CDO Station: Transformed {} stations for state_fips={}",
          output.size(), stateFips);
      return output.toString();

    } catch (Exception e) {
      LOGGER.error("CDO Station: Failed to parse response for {}: {}",
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
}
