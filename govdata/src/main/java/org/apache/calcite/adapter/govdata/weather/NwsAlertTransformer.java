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
 * Transforms NWS alert GeoJSON FeatureCollection into a flat JSON array.
 *
 * <p>Input format (GeoJSON):
 * <pre>{@code
 * {
 *   "features": [{
 *     "properties": {
 *       "id": "urn:oid:2.49.0.1.840.0...",
 *       "event": "Tornado Warning",
 *       "severity": "Extreme",
 *       "certainty": "Observed",
 *       "urgency": "Immediate",
 *       "headline": "...",
 *       "description": "...",
 *       "onset": "2024-01-15T14:00:00-06:00",
 *       "expires": "2024-01-15T15:00:00-06:00",
 *       "senderName": "NWS Chicago IL",
 *       "affectedZones": ["https://...zone/ILZ014", "https://...zone/ILZ015"]
 *     }
 *   }]
 * }
 * }</pre>
 */
public class NwsAlertTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NwsAlertTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("NWS Alert: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode features = root.get("features");
      if (features == null || !features.isArray()) {
        LOGGER.warn("NWS Alert: No 'features' array in response for {}",
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
        row.put("alert_id", getTextOrNull(props, "id"));
        row.put("state_abbr", stateAbbr);
        row.put("event", getTextOrNull(props, "event"));
        row.put("severity", getTextOrNull(props, "severity"));
        row.put("certainty", getTextOrNull(props, "certainty"));
        row.put("urgency", getTextOrNull(props, "urgency"));
        row.put("headline", getTextOrNull(props, "headline"));
        row.put("description", getTextOrNull(props, "description"));
        row.put("onset", getTextOrNull(props, "onset"));
        row.put("expires", getTextOrNull(props, "expires"));
        row.put("sender_name", getTextOrNull(props, "senderName"));

        // Flatten affectedZones array to comma-separated zone IDs
        JsonNode zones = props.get("affectedZones");
        if (zones != null && zones.isArray() && zones.size() > 0) {
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < zones.size(); i++) {
            if (i > 0) {
              sb.append(",");
            }
            String zoneUrl = zones.get(i).asText();
            int lastSlash = zoneUrl.lastIndexOf('/');
            if (lastSlash >= 0 && lastSlash < zoneUrl.length() - 1) {
              sb.append(zoneUrl.substring(lastSlash + 1));
            } else {
              sb.append(zoneUrl);
            }
          }
          row.put("affected_zones", sb.toString());
        } else {
          row.putNull("affected_zones");
        }

        result.add(row);
      }

      LOGGER.debug("NWS Alert: Transformed {} alerts for state={}",
          result.size(), stateAbbr);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("NWS Alert: Failed to parse response for {}: {}",
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
