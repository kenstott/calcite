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
 * Transforms USDA FS ArcGIS MapServer responses into {@code national_forests} rows.
 *
 * <p>Input: ArcGIS query JSON from {@code EDW_ForestSystemBoundaries_01/MapServer/0/query}.
 * The MapServer returns lowercase field names. {@code STATE} and {@code PROCLAIMED_ACRES}
 * are no longer available in this endpoint.
 * <pre>
 * {
 *   "features": [
 *     {
 *       "attributes": {
 *         "forestnumber": "0501",
 *         "forestname": "Angeles National Forest",
 *         "region": "05",
 *         "gis_acres": 694516.4
 *       }
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>Output: JSON array string with columns matching the {@code national_forests} schema.
 */
public class UsfsForestBoundaryTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UsfsForestBoundaryTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("national_forests: empty response from USDA FS ArcGIS");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      if (root.path("exceededTransferLimit").asBoolean(false)) {
        throw new RuntimeException(
            "national_forests: ArcGIS response exceeded transfer limit — "
                + "increase resultRecordCount in lands-schema.yaml or add resultOffset pagination");
      }

      JsonNode features = root.path("features");
      if (!features.isArray()) {
        LOGGER.warn("national_forests: no 'features' array in ArcGIS response");
        return "[]";
      }

      for (JsonNode feature : features) {
        JsonNode attrs = feature.path("attributes");
        if (attrs.isMissingNode()) {
          continue;
        }

        ObjectNode row = MAPPER.createObjectNode();
        row.put("forest_id", textOrNull(attrs, "forestnumber"));
        row.put("forest_name", textOrNull(attrs, "forestname"));
        row.put("region", textOrNull(attrs, "region"));
        row.put("gross_acres", doubleOrNull(attrs, "gis_acres"));
        result.add(row);
      }

      LOGGER.debug("national_forests: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("national_forests: failed to transform ArcGIS response: {}", e.getMessage(), e);
      throw new RuntimeException("national_forests transform failed", e);
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
}
