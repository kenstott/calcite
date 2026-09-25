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

import java.util.HashMap;
import java.util.Map;

/**
 * Transforms the BTS NTAD North American Rail Network (Class I Railroads view) ArcGIS
 * FeatureServer statistics JSON into {@code class1_rail_county_adjacency} rows.
 *
 * <p>Called once per page by the framework OFFSET paginator; it flattens
 * {@code features[].attributes} to one row per (county, Class I railroad) pair. The source query
 * already does the server-side {@code GROUP BY STCNTYFIPS, RROWNER1} with {@code SUM(MILES)} /
 * {@code COUNT(OBJECTID)} — this transformer only reshapes the response and derives
 * {@code state_fips} from {@code STCNTYFIPS} (the view's own separate {@code STFIPS}/
 * {@code CNTYFIPS} fields carry inconsistent formatting on a handful of segments — postal
 * abbreviations instead of numeric FIPS in {@code STFIPS}, unpadded digits in {@code CNTYFIPS} —
 * so grouping or deriving from those directly produces spurious duplicate county rows; the
 * 5-digit {@code STCNTYFIPS} does not have this problem and is authoritative). Rows with a null
 * {@code STCNTYFIPS} are the network's Canadian mileage (no U.S. county FIPS) and are skipped.
 *
 * <p>Unlike {@link FaaAirportsTransformer} the WHERE clause already restricts the source view to
 * the current AAR Class I roster ({@code RROWNER1 IN ('UP','BNSF','CSXT','NS','CN','CPKC')}) —
 * the view's own name ("Class I Railroads") is not itself a reliable filter: live probing found
 * hundreds of short-line/regional/passenger owner codes also present in the underlying segments
 * (confirmed 2026-09-21).
 */
public class Class1RailCountyAdjacencyTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(Class1RailCountyAdjacencyTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Map<String, String> RAILROAD_NAMES = new HashMap<>();
  static {
    RAILROAD_NAMES.put("UP", "Union Pacific Railroad");
    RAILROAD_NAMES.put("BNSF", "BNSF Railway");
    RAILROAD_NAMES.put("CSXT", "CSX Transportation");
    RAILROAD_NAMES.put("NS", "Norfolk Southern Railway");
    RAILROAD_NAMES.put("CN", "Canadian National Railway");
    RAILROAD_NAMES.put("CPKC", "Canadian Pacific Kansas City");
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("class1_rail_county_adjacency: empty response from BTS ArcGIS");
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
        LOGGER.warn("class1_rail_county_adjacency: no 'features' array in ArcGIS response");
        return "[]";
      }
      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode feature : features) {
        JsonNode a = feature.path("attributes");
        if (a.isMissingNode()) {
          continue;
        }
        String countyFips = text(a, "STCNTYFIPS");
        if (countyFips == null || countyFips.length() != 5) {
          // Canadian mileage carries no U.S. county FIPS; not in scope for this table.
          continue;
        }
        String railroadCode = text(a, "RROWNER1");
        ObjectNode row = MAPPER.createObjectNode();
        row.put("state_fips", countyFips.substring(0, 2));
        row.put("county_fips", countyFips);
        row.put("railroad_code", railroadCode);
        String railroadName = RAILROAD_NAMES.get(railroadCode);
        if (railroadName != null) {
          row.put("railroad_name", railroadName);
        } else {
          row.putNull("railroad_name");
        }
        putDouble(row, "total_miles", a, "TOTAL_MILES");
        putInt(row, "segment_count", a, "SEGMENT_COUNT");
        result.add(row);
      }
      LOGGER.debug("class1_rail_county_adjacency: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("class1_rail_county_adjacency: failed to transform ArcGIS response: {}",
          e.getMessage(), e);
      throw new RuntimeException("class1_rail_county_adjacency transform failed", e);
    }
  }

  private static String text(JsonNode a, String field) {
    JsonNode v = a.path(field);
    return v.isNull() || v.isMissingNode() || v.asText().isEmpty() ? null : v.asText();
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
