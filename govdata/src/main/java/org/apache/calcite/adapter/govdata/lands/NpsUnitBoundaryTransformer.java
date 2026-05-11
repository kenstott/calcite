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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms NPS ArcGIS FeatureServer responses into {@code nps_units} rows.
 *
 * <p>Input: ArcGIS query JSON from
 * {@code NPS_Land_Resources/MapServer/2/query?where=1%3D1&outFields=*&f=json}
 * <pre>
 * {
 *   "features": [
 *     {
 *       "attributes": {
 *         "UNIT_CODE": "YOSE",
 *         "UNIT_NAME": "Yosemite National Park",
 *         "UNIT_TYPE": "National Park",
 *         "STATE_FIPS": "06",
 *         "COUNTY_FIPS": "06109",
 *         "GIS_ACRES": 748036.0,
 *         "DATE_EST": 1283212800000,
 *         "REGION": "Pacific West"
 *       }
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>Output: JSON array string with columns matching the {@code nps_units} schema.
 */
public class NpsUnitBoundaryTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NpsUnitBoundaryTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("nps_units: empty response from NPS ArcGIS");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      if (root.path("exceededTransferLimit").asBoolean(false)) {
        throw new RuntimeException(
            "nps_units: ArcGIS response exceeded transfer limit — "
                + "increase resultRecordCount in lands-schema.yaml or add resultOffset pagination");
      }

      JsonNode features = root.path("features");
      if (!features.isArray()) {
        LOGGER.warn("nps_units: no 'features' array in ArcGIS response");
        return "[]";
      }

      for (JsonNode feature : features) {
        JsonNode attrs = feature.path("attributes");
        if (attrs.isMissingNode()) {
          continue;
        }

        ObjectNode row = MAPPER.createObjectNode();
        row.put("unit_code", textOrNull(attrs, "UNIT_CODE"));
        row.put("unit_name", textOrNull(attrs, "UNIT_NAME"));
        row.put("unit_type", textOrNull(attrs, "UNIT_TYPE"));
        row.put("state_fips", textOrNull(attrs, "STATE_FIPS"));
        row.put("county_fips", textOrNull(attrs, "COUNTY_FIPS"));
        row.put("gross_acres", doubleOrNull(attrs, "GIS_ACRES"));
        row.put("established_date", epochToDate(attrs, "DATE_EST"));
        row.put("region", textOrNull(attrs, "REGION"));
        row.putNull("geometry_wkt");
        result.add(row);
      }

      LOGGER.debug("nps_units: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("nps_units: failed to transform ArcGIS response: {}", e.getMessage(), e);
      throw new RuntimeException("nps_units transform failed", e);
    }
  }

  private String textOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    return val.isNull() || val.isMissingNode() ? null : val.asText(null);
  }

  private Double doubleOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    return val.asDouble();
  }

  private String epochToDate(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    if (val.isNumber()) {
      long epochMs = val.asLong();
      if (epochMs == 0) {
        return null;
      }
      return java.time.Instant.ofEpochMilli(epochMs)
          .atZone(java.time.ZoneOffset.UTC)
          .toLocalDate()
          .toString();
    }
    return val.asText(null);
  }
}
