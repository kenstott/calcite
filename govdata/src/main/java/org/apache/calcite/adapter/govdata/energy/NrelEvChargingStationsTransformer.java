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

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Iterator;

/**
 * Maps NREL Alternative Fuels Data Center ({@code /api/alt-fuel-stations/v1.json},
 * filtered to {@code fuel_type=ELEC}) records into {@code ev_charging_stations} rows.
 *
 * <p>The response envelope is {@code {"total_results": N, "fuel_stations": [...]}} — flat
 * per-station objects, unlike FDIC's element-wrapped {@code {"data": {...}}} shape. AFDC
 * already emits ISO-8601 dates/timestamps, so no source-specific date reformatting is
 * needed beyond validating the value parses.
 */
public class NrelEvChargingStationsTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NrelEvChargingStationsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode stations = root.path("fuel_stations");
      if (!stations.isArray()) {
        JsonNode errors = root.path("errors");
        if (errors.isArray() && errors.size() > 0) {
          throw new RuntimeException("NREL AFDC API error: " + errors);
        }
        LOGGER.warn("NrelEvChargingStations: no 'fuel_stations' array in response (first 200 chars: {})",
            response.substring(0, Math.min(200, response.length())));
        return "[]";
      }

      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode s : stations) {
        if (!s.isObject()) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        putLong(row, "id", s, "id");
        putText(row, "station_name", s, "station_name");
        putText(row, "street_address", s, "street_address");
        putText(row, "intersection_directions", s, "intersection_directions");
        putText(row, "city", s, "city");
        putText(row, "state_abbr", s, "state");
        putText(row, "zip", s, "zip");
        putText(row, "country", s, "country");
        putDouble(row, "latitude", s, "latitude");
        putDouble(row, "longitude", s, "longitude");
        putText(row, "access_code", s, "access_code");
        putText(row, "access_days_time", s, "access_days_time");
        putText(row, "access_detail_code", s, "access_detail_code");
        putText(row, "groups_with_access_code", s, "groups_with_access_code");
        putText(row, "facility_type", s, "facility_type");
        putText(row, "owner_type_code", s, "owner_type_code");
        putText(row, "status_code", s, "status_code");
        putDate(row, "open_date", s, "open_date");
        putDate(row, "date_last_confirmed", s, "date_last_confirmed");
        putDate(row, "expected_date", s, "expected_date");
        putText(row, "updated_at", s, "updated_at");
        putConnectorTypes(row, s);
        putInt(row, "ev_dc_fast_num", s, "ev_dc_fast_num");
        putInt(row, "ev_level1_evse_num", s, "ev_level1_evse_num");
        putInt(row, "ev_level2_evse_num", s, "ev_level2_evse_num");
        putText(row, "ev_network", s, "ev_network");
        putText(row, "ev_network_web", s, "ev_network_web");
        putText(row, "ev_pricing", s, "ev_pricing");
        putBool(row, "ev_workplace_charging", s, "ev_workplace_charging");
        putText(row, "ev_renewable_source", s, "ev_renewable_source");
        putBool(row, "restricted_access", s, "restricted_access");
        putText(row, "maximum_vehicle_class", s, "maximum_vehicle_class");
        putText(row, "station_phone", s, "station_phone");
        putText(row, "geocode_status", s, "geocode_status");
        out.add(row);
      }
      LOGGER.debug("NrelEvChargingStations: transformed {} records", out.size());
      return MAPPER.writeValueAsString(out);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("NrelEvChargingStations transform failed: " + e.getMessage(), e);
    }
  }

  /** {@code ev_connector_types} arrives as a JSON array of connector codes; joined comma-separated. */
  private static void putConnectorTypes(ObjectNode row, JsonNode s) {
    JsonNode arr = s.path("ev_connector_types");
    if (!arr.isArray() || arr.size() == 0) {
      row.putNull("ev_connector_types");
      return;
    }
    StringBuilder sb = new StringBuilder();
    for (Iterator<JsonNode> it = arr.elements(); it.hasNext();) {
      JsonNode v = it.next();
      if (v.isNull()) {
        continue;
      }
      if (sb.length() > 0) {
        sb.append(',');
      }
      sb.append(v.asText());
    }
    row.put("ev_connector_types", sb.toString());
  }

  private static void putText(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
    } else {
      row.put(col, v.asText());
    }
  }

  private static void putInt(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull() || (v.isTextual() && v.asText().isEmpty())) {
      row.putNull(col);
    } else {
      row.put(col, v.asInt());
    }
  }

  private static void putLong(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
    } else {
      row.put(col, v.asLong());
    }
  }

  private static void putDouble(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
    } else {
      row.put(col, v.asDouble());
    }
  }

  private static void putBool(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
    } else if (v.isBoolean()) {
      row.put(col, v.booleanValue());
    } else {
      row.put(col, Boolean.parseBoolean(v.asText()));
    }
  }

  /** AFDC dates are already ISO {@code yyyy-MM-dd}; validated (not reformatted) before passthrough. */
  private static void putDate(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
      return;
    }
    String s = v.asText().trim();
    if (s.isEmpty()) {
      row.putNull(col);
      return;
    }
    try {
      LocalDate.parse(s);
      row.put(col, s);
    } catch (DateTimeParseException e) {
      LOGGER.warn("NrelEvChargingStations: unparseable date '{}' in field '{}', storing null", s, field);
      row.putNull(col);
    }
  }
}
