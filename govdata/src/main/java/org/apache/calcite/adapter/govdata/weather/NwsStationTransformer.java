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
 * Transforms NWS station GeoJSON FeatureCollection into a flat JSON array.
 *
 * <p>Input format (GeoJSON):
 * <pre>{@code
 * {
 *   "features": [{
 *     "properties": {
 *       "stationIdentifier": "KORD",
 *       "name": "Chicago O'Hare",
 *       "timeZone": "America/Chicago",
 *       "forecast": "https://api.weather.gov/zones/forecast/ILZ014",
 *       "county": "https://api.weather.gov/zones/county/ILC031",
 *       "elevation": {"value": 201.8, "unitCode": "wmoUnit:m"}
 *     },
 *     "geometry": {"type": "Point", "coordinates": [-87.9, 41.98]}
 *   }]
 * }
 * }</pre>
 */
public class NwsStationTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NwsStationTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("NWS Station: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode features = root.get("features");
      if (features == null || !features.isArray()) {
        LOGGER.warn("NWS Station: No 'features' array in response for {}",
            context.getUrl());
        return "[]";
      }

      String stateAbbr = context.getDimensionValues().get("state_abbr");
      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode feature : features) {
        JsonNode props = feature.get("properties");
        if (props == null) {
          continue;
        }

        ObjectNode row = MAPPER.createObjectNode();
        row.put("station_id", getTextOrNull(props, "stationIdentifier"));
        row.put("station_name", getTextOrNull(props, "name"));
        row.put("state_abbr", stateAbbr);

        // Extract coordinates from geometry
        JsonNode geometry = feature.get("geometry");
        if (geometry != null) {
          JsonNode coords = geometry.get("coordinates");
          if (coords != null && coords.isArray() && coords.size() >= 2) {
            row.put("longitude", coords.get(0).doubleValue());
            row.put("latitude", coords.get(1).doubleValue());
          } else {
            row.putNull("longitude");
            row.putNull("latitude");
          }
        } else {
          row.putNull("longitude");
          row.putNull("latitude");
        }

        // Elevation from nested object
        JsonNode elevation = props.get("elevation");
        if (elevation != null && elevation.isObject()) {
          JsonNode elevValue = elevation.get("value");
          if (elevValue != null && elevValue.isNumber()) {
            row.put("elevation_m", elevValue.doubleValue());
          } else {
            row.putNull("elevation_m");
          }
        } else {
          row.putNull("elevation_m");
        }

        row.put("timezone", getTextOrNull(props, "timeZone"));

        // Extract zone IDs from URLs
        row.put("forecast_zone", extractZoneId(props, "forecast"));
        row.put("county_zone", extractZoneId(props, "county"));

        result.add(row);
      }

      LOGGER.debug("NWS Station: Transformed {} stations for state={}",
          result.size(), stateAbbr);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("NWS Station: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  /**
   * Extracts a zone ID from a URL like
   * "https://api.weather.gov/zones/forecast/ILZ014" -> "ILZ014".
   */
  static String extractZoneId(JsonNode props, String field) {
    String url = getTextOrNull(props, field);
    if (url == null) {
      return null;
    }
    int lastSlash = url.lastIndexOf('/');
    if (lastSlash >= 0 && lastSlash < url.length() - 1) {
      return url.substring(lastSlash + 1);
    }
    return url;
  }

  private static String getTextOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asText();
  }
}
