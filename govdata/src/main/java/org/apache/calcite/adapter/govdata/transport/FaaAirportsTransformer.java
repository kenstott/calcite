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
 * Transforms FAA NTAD Aviation-Facilities ArcGIS FeatureServer JSON into
 * {@code airports} rows.
 *
 * <p>Called once per page by the framework OFFSET paginator (pageSize 2000 =
 * ArcGIS {@code maxRecordCount}); it flattens {@code features[].attributes} to
 * one row per facility. Unlike the lands ArcGIS transformers it must <b>not</b>
 * throw on {@code exceededTransferLimit} — that flag is true on every full page
 * and page termination is handled by the paginator (a short final page).
 */
public class FaaAirportsTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FaaAirportsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("airports: empty response from FAA ArcGIS");
      return "[]";
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode error = root.path("error");
      if (!error.isMissingNode() && !error.isNull()) {
        throw new RuntimeException("FAA ArcGIS error: " + error.toString());
      }
      JsonNode features = root.path("features");
      if (!features.isArray()) {
        LOGGER.warn("airports: no 'features' array in ArcGIS response");
        return "[]";
      }
      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode feature : features) {
        JsonNode a = feature.path("attributes");
        if (a.isMissingNode()) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        putText(row, "arpt_id", a, "ARPT_ID");
        putText(row, "arpt_name", a, "ARPT_NAME");
        putText(row, "city", a, "CITY");
        putText(row, "state_code", a, "STATE_CODE");
        putText(row, "state_name", a, "STATE_NAME");
        putText(row, "county_name", a, "COUNTY_NAME");
        putDouble(row, "latitude", a, "LAT_DECIMAL");
        putDouble(row, "longitude", a, "LONG_DECIMAL");
        putDouble(row, "elevation_ft", a, "ELEV");
        putText(row, "site_type_code", a, "SITE_TYPE_CODE");
        putText(row, "facility_use_code", a, "FACILITY_USE_CODE");
        putText(row, "ownership_type_code", a, "OWNERSHIP_TYPE_CODE");
        putText(row, "arpt_status", a, "ARPT_STATUS");
        putText(row, "region_code", a, "REGION_CODE");
        putText(row, "eff_date", a, "EFF_DATE");
        result.add(row);
      }
      LOGGER.debug("airports: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("airports: failed to transform ArcGIS response: {}", e.getMessage(), e);
      throw new RuntimeException("airports transform failed", e);
    }
  }

  private static void putText(ObjectNode row, String col, JsonNode a, String field) {
    JsonNode v = a.path(field);
    if (v.isNull() || v.isMissingNode() || v.asText().isEmpty()) {
      row.putNull(col);
    } else {
      row.put(col, v.asText());
    }
  }

  private static void putDouble(ObjectNode row, String col, JsonNode a, String field) {
    JsonNode v = a.path(field);
    if (v.isNumber()) {
      row.put(col, v.asDouble());
    } else {
      row.putNull(col);
    }
  }
}
