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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms ArcGIS FeatureServer query responses into {@code gas_pipelines} rows.
 *
 * <p>Input: one page of Esri query JSON from
 * {@code Natural_Gas_Interstate_and_Intrastate_Pipelines_1/FeatureServer/0/query} (source is EIA,
 * mirrored under Esri's Federal_User_Community org, "checked monthly for updates" per the item's
 * own listing). The query requests {@code outSR=4326} so {@code geometry.paths} are already WGS84
 * degrees; {@link EsriPolylineGeometryConverter} converts them to WKT. Called once per page by
 * {@code HttpSource}'s OFFSET pagination.
 * <pre>
 * {
 *   "features": [
 *     {
 *       "attributes": {
 *         "FID": 1, "TYPEPIPE": "Interstate", "Operator": "...",
 *         "Status": "Operating", "Shape__Length": 12181.74
 *       },
 *       "geometry": { "paths": [ [ [x, y], [x, y], ... ] ] }
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>Output: JSON array string with columns matching the {@code gas_pipelines} schema.
 */
public class GasPipelineTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GasPipelineTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("gas_pipelines: empty response from ArcGIS");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      JsonNode features = root.path("features");
      if (!features.isArray()) {
        LOGGER.warn("gas_pipelines: no 'features' array in ArcGIS response");
        return "[]";
      }

      for (JsonNode feature : features) {
        JsonNode attrs = feature.path("attributes");
        if (attrs.isMissingNode()) {
          continue;
        }

        ObjectNode row = MAPPER.createObjectNode();
        row.put("segment_id", intOrNull(attrs, "FID"));
        row.put("pipeline_type", textOrNull(attrs, "TYPEPIPE"));
        row.put("operator", textOrNull(attrs, "Operator"));
        row.put("status", textOrNull(attrs, "Status"));
        row.put("length_m", doubleOrNull(attrs, "Shape__Length"));
        row.put("geometry_wkt", EsriPolylineGeometryConverter.convert(feature.path("geometry")));
        result.add(row);
      }

      LOGGER.debug("gas_pipelines: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("gas_pipelines: failed to transform ArcGIS response: {}", e.getMessage(), e);
      throw new RuntimeException("gas_pipelines transform failed", e);
    }
  }

  private String textOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    return val.isNull() || val.isMissingNode() ? null : val.asText(null);
  }

  private Integer intOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    return val.asInt();
  }

  private Double doubleOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    return val.asDouble();
  }
}
