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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms the BTS NTAD National Highway System ArcGIS FeatureServer statistics JSON into
 * {@code nhs_highway_county_access} rows.
 *
 * <p>Called once per page by the framework OFFSET paginator; it flattens {@code
 * features[].attributes} to one row per county. The source query already does the server-side
 * {@code GROUP BY STFIPS, CTFIPS} with {@code SUM(MILES)} / {@code COUNT(OBJECTID)} — this
 * transformer only reshapes the response and derives the 5-digit {@code county_fips} from the
 * layer's separate {@code STFIPS} (state, no leading zero) and {@code CTFIPS} (county, no leading
 * zeros) numeric fields, zero-padding each to 2 and 3 digits respectively. The WHERE clause
 * already excludes {@code CTFIPS=0} (3 placeholder segments with zero mileage and blank route
 * names, verified live 2026-09-21) so every group here is a real county.
 */
public class NhsHighwayCountyAccessTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NhsHighwayCountyAccessTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("nhs_highway_county_access: empty response from BTS ArcGIS");
      return "[]";
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode error = root.path("error");
      if (!error.isMissingNode() && !error.isNull()) {
        throw new RuntimeException("BTS ArcGIS error: " + error.toString());
      }
      JsonNode features = root.path("features");
      if (!features.isArray()) {
        LOGGER.warn("nhs_highway_county_access: no 'features' array in ArcGIS response");
        return "[]";
      }
      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode feature : features) {
        JsonNode a = feature.path("attributes");
        if (a.isMissingNode()) {
          continue;
        }
        Integer stfips = intValue(a, "STFIPS");
        Integer ctfips = intValue(a, "CTFIPS");
        if (stfips == null || ctfips == null) {
          continue;
        }
        String stateFips = String.format("%02d", stfips);
        String countyFips = stateFips + String.format("%03d", ctfips);
        ObjectNode row = MAPPER.createObjectNode();
        row.put("state_fips", stateFips);
        row.put("county_fips", countyFips);
        putDouble(row, "total_miles", a, "TOTAL_MILES");
        putInt(row, "segment_count", a, "SEGMENT_COUNT");
        result.add(row);
      }
      LOGGER.debug("nhs_highway_county_access: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("nhs_highway_county_access: failed to transform ArcGIS response: {}",
          e.getMessage(), e);
      throw new RuntimeException("nhs_highway_county_access transform failed", e);
    }
  }

  private static Integer intValue(JsonNode a, String field) {
    JsonNode v = a.path(field);
    return v.isNumber() ? v.asInt() : null;
  }

  private static void putDouble(ObjectNode row, String col, JsonNode a, String field) {
    JsonNode v = a.path(field);
    if (v.isNumber()) {
      row.put(col, v.asDouble());
    } else {
      row.putNull(col);
    }
  }

  private static void putInt(ObjectNode row, String col, JsonNode a, String field) {
    JsonNode v = a.path(field);
    if (v.isNumber()) {
      row.put(col, v.asInt());
    } else {
      row.putNull(col);
    }
  }
}
