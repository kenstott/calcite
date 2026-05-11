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
 * <p>Input: ArcGIS query JSON from the NPS hosted feature service.
 * {@code STATE} is a 2-character state abbreviation (not FIPS). {@code Shape__Area} is in
 * square meters and is converted to acres by dividing by 4046.856.
 * {@code GIS_ACRES}, {@code DATE_EST}, and {@code COUNTY_FIPS} are not available.
 * <pre>
 * {
 *   "features": [
 *     {
 *       "attributes": {
 *         "UNIT_CODE": "YOSE",
 *         "UNIT_NAME": "Yosemite National Park",
 *         "UNIT_TYPE": "National Parks",
 *         "STATE": "CA",
 *         "REGION": "Pacific West",
 *         "Shape__Area": 9640234567.0
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

  private static final double SQ_METERS_PER_ACRE = 4046.856;

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
        row.put("state_abbr", textOrNull(attrs, "STATE"));
        row.put("region", textOrNull(attrs, "REGION"));
        row.put("gross_acres", sqMetersToAcres(attrs, "Shape__Area"));
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

  private Double sqMetersToAcres(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    return val.asDouble() / SQ_METERS_PER_ACRE;
  }
}
