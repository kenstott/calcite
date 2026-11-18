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
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Transforms NRCS AWDB REST API (wcc.sc.egov.usda.gov/awdbRestApi) responses for SNOTEL into flat
 * JSON rows. One class serves two tables, selected by the {@code type} dimension:
 *
 * <ul>
 *   <li>{@code snotel_station}: {@code /stations} list, one row per SNOTEL station.</li>
 *   <li>{@code snotel_daily}: {@code /data} for WTEQ, SNWD and PREC, DAILY duration, one state
 *       and one calendar year per request. The API returns one block per station and one
 *       {@code data} entry per element; the elements are merged into one row per station-date.
 *       WTEQ and PREC values carry the official 1991-2020 daily {@code median}.</li>
 * </ul>
 *
 * <p>{@code wteq_pct_median} is 100 * wteq / median and is null when the median is absent or zero
 * (the ratio is undefined there, e.g. a mid-summer median of 0).
 */
public class SnotelTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    String type = context.getDimensionValues().get("type");
    if (type == null) {
      throw new IllegalStateException("SNOTEL: type dimension missing for " + context.getUrl());
    }
    JsonNode root;
    try {
      root = MAPPER.readTree(response);
    } catch (IOException e) {
      throw new RuntimeException("SNOTEL: unparseable response for " + context.getUrl(), e);
    }
    if (root == null || !root.isArray()) {
      throw new IllegalStateException("SNOTEL: expected a JSON array from " + context.getUrl());
    }
    if ("snotel_station".equals(type)) {
      return stations(root).toString();
    }
    if ("snotel_daily".equals(type)) {
      String state = context.getDimensionValues().get("state_abbr");
      return daily(root, state).toString();
    }
    throw new IllegalStateException("SNOTEL: unknown type " + type);
  }

  private static ArrayNode stations(JsonNode root) {
    ArrayNode out = MAPPER.createArrayNode();
    for (JsonNode s : root) {
      ObjectNode row = MAPPER.createObjectNode();
      row.put("station_id", s.path("stationTriplet").asText());
      row.put("station_code", s.path("stationId").asText());
      row.put("state_abbr", s.path("stateCode").asText());
      row.put("station_name", s.path("name").asText());
      putText(row, "county_name", s.path("countyName"));
      putText(row, "shef_id", s.path("shefId"));
      String huc = s.path("huc").asText(null);
      putString(row, "huc12", huc);
      putString(row, "huc8", huc != null && huc.length() >= 8 ? huc.substring(0, 8) : null);
      putString(row, "huc6", huc != null && huc.length() >= 6 ? huc.substring(0, 6) : null);
      putNumber(row, "elevation_ft", s.path("elevation"));
      putNumber(row, "latitude", s.path("latitude"));
      putNumber(row, "longitude", s.path("longitude"));
      putDate(row, "begin_date", s.path("beginDate"));
      putDate(row, "end_date", s.path("endDate"));
      out.add(row);
    }
    return out;
  }

  private static ArrayNode daily(JsonNode root, String state) {
    ArrayNode out = MAPPER.createArrayNode();
    for (JsonNode station : root) {
      String triplet = station.path("stationTriplet").asText();
      Map<String, ObjectNode> byDate = new TreeMap<String, ObjectNode>();
      for (JsonNode element : station.path("data")) {
        String code = element.path("stationElement").path("elementCode").asText();
        String valueCol;
        String medianCol;
        if ("WTEQ".equals(code)) {
          valueCol = "wteq_in";
          medianCol = "wteq_median_in";
        } else if ("SNWD".equals(code)) {
          valueCol = "snwd_in";
          medianCol = null;
        } else if ("PREC".equals(code)) {
          valueCol = "prec_accum_in";
          medianCol = "prec_accum_median_in";
        } else {
          throw new IllegalStateException("SNOTEL: unexpected element " + code + " for " + triplet);
        }
        for (JsonNode v : element.path("values")) {
          String date = v.path("date").asText();
          ObjectNode row = byDate.get(date);
          if (row == null) {
            row = MAPPER.createObjectNode();
            row.put("station_id", triplet);
            row.put("state_abbr", state);
            row.put("date", date);
            row.put("year", Integer.parseInt(date.substring(0, 4)));
            byDate.put(date, row);
          }
          putNumber(row, valueCol, v.path("value"));
          if (medianCol != null) {
            putNumber(row, medianCol, v.path("median"));
          }
        }
      }
      for (ObjectNode row : byDate.values()) {
        JsonNode wteq = row.get("wteq_in");
        JsonNode med = row.get("wteq_median_in");
        if (wteq != null && med != null && med.asDouble() > 0) {
          row.put("wteq_pct_median", Math.round(1000.0 * wteq.asDouble() / med.asDouble()) / 10.0);
        }
        out.add(fill(row));
      }
    }
    return out;
  }

  private static final String[] DAILY_COLUMNS = {
      "station_id", "state_abbr", "date", "year", "wteq_in", "wteq_median_in", "wteq_pct_median",
      "snwd_in", "prec_accum_in", "prec_accum_median_in"};

  /** Emits every column explicitly so a missing element is a null, in a stable column order. */
  private static ObjectNode fill(ObjectNode row) {
    ObjectNode full = MAPPER.createObjectNode();
    for (String c : DAILY_COLUMNS) {
      JsonNode v = row.get(c);
      if (v == null) {
        full.putNull(c);
      } else {
        full.set(c, v);
      }
    }
    return full;
  }

  private static void putText(ObjectNode row, String col, JsonNode v) {
    putString(row, col, v.isMissingNode() || v.isNull() ? null : v.asText());
  }

  private static void putString(ObjectNode row, String col, String v) {
    if (v == null) {
      row.putNull(col);
    } else {
      row.put(col, v);
    }
  }

  private static void putNumber(ObjectNode row, String col, JsonNode v) {
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
    } else {
      row.put(col, v.asDouble());
    }
  }

  /** "1979-10-01 00:00" becomes "1979-10-01"; the 2100-01-01 open-end sentinel becomes null. */
  private static void putDate(ObjectNode row, String col, JsonNode v) {
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
      return;
    }
    String d = v.asText().substring(0, 10);
    if (d.startsWith("2100-")) {
      row.putNull(col);
    } else {
      row.put(col, d);
    }
  }
}
