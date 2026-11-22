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
package org.apache.calcite.adapter.govdata.disasters;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.ZoneOffset;

/**
 * Base for FEMA National Flood Hazard Layer (NFHL) ArcGIS MapServer transformers. Each page is an
 * ArcGIS JSON query response ({@code features[].attributes} plus WGS84 polygon {@code rings});
 * subclasses add their layer's attribute columns, this base adds the keys every NFHL row shares:
 * state/county FIPS from {@code DFIRM_ID} and the polygon's bounding box.
 */
abstract class AbstractFemaNfhlTransformer implements ResponseTransformer {

  /**
   * FEMA encodes "no date" as placeholder years (9999 in PRE_DATE, 8888-08-08 in some EFF_DATE);
   * no real flood-map date is past year 3000, so anything at or beyond it is null.
   */
  private static final long NO_DATE_THRESHOLD_MS = 32_503_680_000_000L;

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /** Layer name used in error messages. */
  protected abstract String tableName();

  /** Copies the layer-specific attributes into {@code row}. */
  protected abstract void mapAttributes(JsonNode attrs, ObjectNode row);

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new RuntimeException(tableName() + ": empty response from FEMA NFHL");
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode error = root.path("error");
      if (error.isObject()) {
        throw new RuntimeException(tableName() + ": FEMA NFHL error " + error);
      }
      JsonNode features = root.path("features");
      if (!features.isArray()) {
        throw new RuntimeException(tableName() + ": no 'features' array in response");
      }
      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode feature : features) {
        JsonNode attrs = feature.path("attributes");
        String dfirm = text(attrs, "DFIRM_ID");
        if (dfirm == null || !dfirm.matches("[0-9]{5}.")) {
          throw new RuntimeException(tableName() + ": DFIRM_ID '" + dfirm
              + "' is not 5 digits plus a suffix letter (OBJECTID " + attrs.path("OBJECTID") + ")");
        }
        ObjectNode row = MAPPER.createObjectNode();
        String globalId = text(attrs, "GlobalID");
        if (globalId == null) {
          throw new RuntimeException(tableName() + ": feature without GlobalID (OBJECTID "
              + attrs.path("OBJECTID") + ")");
        }
        row.put("global_id", globalId.replace("{", "").replace("}", ""));
        row.put("dfirm_id", dfirm);
        row.put("state_fips", dfirm.substring(0, 2));
        row.put("county_fips", dfirm.substring(0, 5));
        mapAttributes(attrs, row);
        addBoundingBox(feature.path("geometry").path("rings"), row);
        result.add(row);
      }
      return MAPPER.writeValueAsString(result);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(tableName() + " transform failed", e);
    }
  }

  private void addBoundingBox(JsonNode rings, ObjectNode row) {
    if (!rings.isArray() || rings.size() == 0) {
      // FEMA publishes some records (~1.4% of LOMRs) with no polygon; the bbox is genuinely absent.
      for (String c : new String[] {"min_lon", "min_lat", "max_lon", "max_lat", "center_lon",
          "center_lat"}) {
        row.putNull(c);
      }
      return;
    }
    double minLon = Double.POSITIVE_INFINITY;
    double maxLon = Double.NEGATIVE_INFINITY;
    double minLat = Double.POSITIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;
    for (JsonNode ring : rings) {
      for (JsonNode pt : ring) {
        double lon = pt.get(0).asDouble();
        double lat = pt.get(1).asDouble();
        minLon = Math.min(minLon, lon);
        maxLon = Math.max(maxLon, lon);
        minLat = Math.min(minLat, lat);
        maxLat = Math.max(maxLat, lat);
      }
    }
    row.put("min_lon", minLon);
    row.put("min_lat", minLat);
    row.put("max_lon", maxLon);
    row.put("max_lat", maxLat);
    row.put("center_lon", (minLon + maxLon) / 2.0);
    row.put("center_lat", (minLat + maxLat) / 2.0);
  }

  protected static String text(JsonNode attrs, String field) {
    JsonNode v = attrs.path(field);
    if (v.isNull() || v.isMissingNode()) {
      return null;
    }
    String s = v.asText().trim();
    return s.isEmpty() ? null : s;
  }

  /** Puts an ArcGIS epoch-millisecond date as an ISO yyyy-MM-dd string, or null for no date. */
  protected static void putDate(ObjectNode row, String column, JsonNode attrs, String field) {
    JsonNode v = attrs.path(field);
    if (!v.isNumber() || v.asLong() >= NO_DATE_THRESHOLD_MS) {
      row.putNull(column);
      return;
    }
    row.put(column, Instant.ofEpochMilli(v.asLong()).atZone(ZoneOffset.UTC).toLocalDate()
        .toString());
  }

  protected static void putText(ObjectNode row, String column, JsonNode attrs, String field) {
    String s = text(attrs, field);
    if (s == null) {
      row.putNull(column);
    } else {
      row.put(column, s);
    }
  }
}
