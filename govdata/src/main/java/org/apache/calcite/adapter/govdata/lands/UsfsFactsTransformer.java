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
 * Transforms USDA FS FACTS ArcGIS MapServer responses into {@code timber_sales} rows.
 *
 * <p>Input: ArcGIS query JSON from {@code EDW_TimberHarvest_01/MapServer/0/query}.
 * This is harvest activity data (activity-level, not sale contracts). The MapServer
 * returns uppercase field names and does not include an {@code exceededTransferLimit} field.
 * <pre>
 * {
 *   "features": [
 *     {
 *       "attributes": {
 *         "facts_id": "OR095000008",
 *         "sale_name": "Desolation Salvage",
 *         "fy_completed": "2022",
 *         "fy_awarded": "2021",
 *         "admin_forest_code": "0601",
 *         "admin_forest_name": "Willamette National Forest",
 *         "admin_district_code": "0605",
 *         "admin_district_name": "McKenzie River Ranger District",
 *         "state_abbr": "OR",
 *         "gis_acres": 45.3,
 *         "activity_code": "6101",
 *         "activity_name": "Timber Sale Preparation",
 *         "treatment_type": "Salvage",
 *         "nbr_units_accomplished": 450.0,
 *         "uom": "CCF",
 *         "cost_per_uom": 85.0
 *       }
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>Output: JSON array string with columns matching the {@code timber_sales} schema.
 */
public class UsfsFactsTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsfsFactsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("timber_sales: empty response from USDA FS FACTS ArcGIS");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      JsonNode features = root.path("features");
      if (!features.isArray()) {
        LOGGER.warn("timber_sales: no 'features' array in ArcGIS response");
        return "[]";
      }

      for (JsonNode feature : features) {
        JsonNode attrs = feature.path("attributes");
        if (attrs.isMissingNode()) {
          continue;
        }

        ObjectNode row = MAPPER.createObjectNode();
        row.put("facts_id", textOrNull(attrs, "facts_id"));
        row.put("sale_name", textOrNull(attrs, "sale_name"));
        row.put("fy_completed", parseIntFromString(attrs, "fy_completed"));
        row.put("fy_awarded", parseIntFromString(attrs, "fy_awarded"));
        row.put("forest_code", textOrNull(attrs, "admin_forest_code"));
        row.put("forest_name", textOrNull(attrs, "admin_forest_name"));
        row.put("district_code", textOrNull(attrs, "admin_district_code"));
        row.put("district_name", textOrNull(attrs, "admin_district_name"));
        row.put("state_abbr", textOrNull(attrs, "state_abbr"));
        row.put("gis_acres", doubleOrNull(attrs, "gis_acres"));
        row.put("activity_code", textOrNull(attrs, "activity_code"));
        row.put("activity_name", textOrNull(attrs, "activity_name"));
        row.put("treatment_type", textOrNull(attrs, "treatment_type"));
        row.put("units_accomplished", doubleOrNull(attrs, "nbr_units_accomplished"));
        row.put("uom", textOrNull(attrs, "uom"));
        row.put("cost_per_uom", doubleOrNull(attrs, "cost_per_uom"));
        result.add(row);
      }

      LOGGER.debug("timber_sales: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("timber_sales: failed to transform ArcGIS response: {}", e.getMessage(), e);
      throw new RuntimeException("timber_sales transform failed", e);
    }
  }

  private String textOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    return val.isNull() || val.isMissingNode() ? null : val.asText(null);
  }

  private Integer parseIntFromString(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    String text = val.asText(null);
    if (text == null || text.trim().isEmpty()) {
      return null;
    }
    try {
      return Integer.parseInt(text.trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("timber_sales: could not parse integer from field {}: {}", field, text);
      return null;
    }
  }

  private Double doubleOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    return val.asDouble();
  }
}
