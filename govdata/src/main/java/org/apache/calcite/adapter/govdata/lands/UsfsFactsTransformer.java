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
 *         "FACTS_ID": "OR095000008",
 *         "SALE_NAME": "Desolation Salvage",
 *         "FY_COMPLETED": "2022",
 *         "FY_AWARDED": "2021",
 *         "ADMIN_FOREST_CODE": "0601",
 *         "ADMIN_FOREST_NAME": "Willamette National Forest",
 *         "ADMIN_DISTRICT_CODE": "0605",
 *         "ADMIN_DISTRICT_NAME": "McKenzie River Ranger District",
 *         "STATE_ABBR": "OR",
 *         "GIS_ACRES": 45.3,
 *         "ACTIVITY_CODE": "6101",
 *         "ACTIVITY_NAME": "Timber Sale Preparation",
 *         "TREATMENT_TYPE": "Salvage",
 *         "NBR_UNITS_ACCOMPLISHED": 450.0,
 *         "UOM": "CCF",
 *         "COST_PER_UOM": 85.0
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
        row.put("facts_id", textOrNull(attrs, "FACTS_ID"));
        row.put("sale_name", textOrNull(attrs, "SALE_NAME"));
        row.put("fy_completed", parseIntFromString(attrs, "FY_COMPLETED"));
        row.put("fy_awarded", parseIntFromString(attrs, "FY_AWARDED"));
        row.put("forest_code", textOrNull(attrs, "ADMIN_FOREST_CODE"));
        row.put("forest_name", textOrNull(attrs, "ADMIN_FOREST_NAME"));
        row.put("district_code", textOrNull(attrs, "ADMIN_DISTRICT_CODE"));
        row.put("district_name", textOrNull(attrs, "ADMIN_DISTRICT_NAME"));
        row.put("state_abbr", textOrNull(attrs, "STATE_ABBR"));
        row.put("gis_acres", doubleOrNull(attrs, "GIS_ACRES"));
        row.put("activity_code", textOrNull(attrs, "ACTIVITY_CODE"));
        row.put("activity_name", textOrNull(attrs, "ACTIVITY_NAME"));
        row.put("treatment_type", textOrNull(attrs, "TREATMENT_TYPE"));
        row.put("units_accomplished", doubleOrNull(attrs, "NBR_UNITS_ACCOMPLISHED"));
        row.put("uom", textOrNull(attrs, "UOM"));
        row.put("cost_per_uom", doubleOrNull(attrs, "COST_PER_UOM"));
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
