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
 * Transforms USDA FIA DataMart REST API responses into {@code forest_inventory} rows.
 *
 * <p>Fetches state-level forest area, volume, and carbon stock estimates from the
 * USDA Forest Inventory and Analysis (FIA) API, which returns aggregated estimates
 * by state, forest type group, and ownership class.
 *
 * <p>Input: FIA DataMart JSON array
 * <pre>
 * [
 *   {
 *     "statecd": "06",
 *     "forest_type_group": "Douglas-fir",
 *     "owngrp_desc": "National Forest",
 *     "land_area": 4817234.5,
 *     "volcfnet": 28340000000.0,
 *     "carbon_ag": 842000000.0,
 *     "tpa_unadj": 185.3,
 *     "balive": 128.7
 *   }
 * ]
 * </pre>
 *
 * <p>Output: JSON array string with columns matching the {@code forest_inventory} schema.
 */
public class FiaDatamartTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaDatamartTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("forest_inventory: empty response from FIA DataMart");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      String inventoryYear = context.getDimensionValues().get("year");
      String stateCode = context.getDimensionValues().get("stateCode");

      JsonNode records = root.isArray() ? root : root.path("data");
      if (!records.isArray()) {
        LOGGER.warn("forest_inventory: expected JSON array from FIA DataMart for state={} year={}, got: {}",
            stateCode, inventoryYear, root.getNodeType());
        return "[]";
      }

      for (JsonNode record : records) {
        ObjectNode row = MAPPER.createObjectNode();

        if (inventoryYear != null) {
          row.put("inventory_year", Integer.parseInt(inventoryYear));
        } else {
          row.putNull("inventory_year");
        }

        row.put("state_fips", padStateFips(textOrNull(record, "statecd")));
        row.put("forest_type_group", textOrNull(record, "forest_type_group"));
        row.put("ownership_class", textOrNull(record, "owngrp_desc"));
        row.put("land_area_acres", doubleOrNull(record, "land_area"));
        row.put("live_volume_cuft", doubleOrNull(record, "volcfnet"));
        row.put("carbon_stock_tons", doubleOrNull(record, "carbon_ag"));
        row.put("trees_per_acre", doubleOrNull(record, "tpa_unadj"));
        row.put("basal_area_sqft", doubleOrNull(record, "balive"));
        result.add(row);
      }

      LOGGER.debug("forest_inventory: transformed {} records for state={} year={}", result.size(), stateCode, inventoryYear);
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("forest_inventory: failed to transform FIA response: {}", e.getMessage(), e);
      throw new RuntimeException("forest_inventory transform failed", e);
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

  private String padStateFips(String raw) {
    if (raw == null) {
      return null;
    }
    return raw.length() == 1 ? "0" + raw : raw;
  }
}
