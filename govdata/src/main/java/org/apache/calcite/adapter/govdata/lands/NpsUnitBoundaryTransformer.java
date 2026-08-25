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
 * {@code STATE} is a 2-character state abbreviation (not FIPS). {@code Shape__Area} is
 * always computed by the service in the layer's storage spatial reference (Web Mercator,
 * EPSG:3857/102100) regardless of any {@code outSR} requested on the query — reprojecting
 * the returned geometry does not change the attribute. Web Mercator inflates area by
 * {@code 1/cos(lat)^2}, so a raw conversion understates every unit and is wrong by 5-6x at
 * Alaska latitudes; this is corrected using the geometry's mean vertex latitude (recovered
 * via the inverse Mercator projection) before converting to acres.
 * {@code GIS_ACRES}, {@code DATE_EST}, and {@code COUNTY_FIPS} are not available on any
 * layer of this FeatureServer.
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
 *       },
 *       "geometry": { "rings": [ [ [x, y], [x, y], ... ] ] }
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
  private static final double WEB_MERCATOR_EARTH_RADIUS_M = 6378137.0;

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
        row.put("gross_acres", sqMetersToAcres(attrs, feature.path("geometry")));
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

  private Double sqMetersToAcres(JsonNode attrs, JsonNode geometry) {
    JsonNode val = attrs.path("Shape__Area");
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    double webMercatorSqMeters = val.asDouble();
    double correction = webMercatorAreaCorrection(geometry);
    return webMercatorSqMeters * correction / SQ_METERS_PER_ACRE;
  }

  /**
   * Web Mercator scales area by {@code 1/cos(lat)^2} relative to the true ellipsoidal area.
   * Returns {@code cos(lat)^2} for the geometry's mean vertex latitude (recovered via the
   * inverse Mercator projection), or {@code 1.0} (no correction) if no ring vertices are
   * available to derive a representative latitude from.
   */
  private double webMercatorAreaCorrection(JsonNode geometry) {
    JsonNode rings = geometry.path("rings");
    if (!rings.isArray() || rings.isEmpty()) {
      return 1.0;
    }
    double ySum = 0.0;
    long yCount = 0;
    for (JsonNode ring : rings) {
      if (!ring.isArray()) {
        continue;
      }
      for (JsonNode vertex : ring) {
        if (vertex.isArray() && vertex.size() >= 2) {
          ySum += vertex.get(1).asDouble();
          yCount++;
        }
      }
    }
    if (yCount == 0) {
      return 1.0;
    }
    double meanY = ySum / yCount;
    double lat = 2 * Math.atan(Math.exp(meanY / WEB_MERCATOR_EARTH_RADIUS_M)) - Math.PI / 2;
    double cosLat = Math.cos(lat);
    return cosLat * cosLat;
  }
}
