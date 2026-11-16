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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Flattens an NHC HURDAT2 best-track file into one row per 6-hourly (or landfall/peak
 * special) observation. The file alternates a storm header line
 * ({@code AL092004, IVAN, 62,}) with that many data lines
 * ({@code 20040902, 1800, , TS, 9.7N, 27.0W, 40, 1006, 0, 0, ...}); wind radii are 12 columns
 * in nautical miles, and -999 / -99 mark a missing value.
 */
public class HurdatBestTrackTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String[] RADII = {
      "wind_34kt_ne_nm", "wind_34kt_se_nm", "wind_34kt_sw_nm", "wind_34kt_nw_nm",
      "wind_50kt_ne_nm", "wind_50kt_se_nm", "wind_50kt_sw_nm", "wind_50kt_nw_nm",
      "wind_64kt_ne_nm", "wind_64kt_se_nm", "wind_64kt_sw_nm", "wind_64kt_nw_nm"};

  @Override public String transform(String response, RequestContext context) {
    String basin = context.getDimensionValues().get("basin");
    ArrayNode out = MAPPER.createArrayNode();
    String stormId = null;
    String stormName = null;
    int season = 0;
    int remaining = 0;
    for (String line : response.split("\r?\n")) {
      if (line.trim().isEmpty()) {
        continue;
      }
      String[] f = line.split(",", -1);
      if (remaining == 0) {
        if (f.length < 3 || !f[0].trim().matches("[A-Z]{2}\\d{6}")) {
          throw new RuntimeException("HURDAT2: expected storm header, got '" + line + "'");
        }
        stormId = f[0].trim();
        stormName = f[1].trim();
        season = Integer.parseInt(stormId.substring(4));
        remaining = Integer.parseInt(f[2].trim());
        continue;
      }
      remaining--;
      if (f.length < 7 + RADII.length) {
        throw new RuntimeException("HURDAT2: short data line for " + stormId + ": '" + line + "'");
      }
      String d = f[0].trim();
      String t = f[1].trim();
      ObjectNode row = MAPPER.createObjectNode();
      row.put("storm_id", stormId);
      row.put("storm_name", stormName);
      row.put("season", season);
      row.put("basin", basin);
      row.put("obs_date", d.substring(0, 4) + "-" + d.substring(4, 6) + "-" + d.substring(6, 8));
      row.put("obs_time_utc", t);
      putText(row, "record_identifier", f[2]);
      row.put("status", f[3].trim());
      row.put("latitude", coord(f[4]));
      row.put("longitude", coord(f[5]));
      putInt(row, "max_wind_kt", f[6]);
      putInt(row, "min_pressure_mb", f[7]);
      for (int i = 0; i < RADII.length; i++) {
        putInt(row, RADII[i], f[8 + i]);
      }
      out.add(row);
    }
    if (remaining != 0 || stormId == null) {
      throw new RuntimeException("HURDAT2: file ended inside a storm (" + stormId + ")");
    }
    try {
      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      throw new RuntimeException("HURDAT2 transform failed: " + e.getMessage(), e);
    }
  }

  private static double coord(String s) {
    String v = s.trim();
    char h = v.charAt(v.length() - 1);
    double x = Double.parseDouble(v.substring(0, v.length() - 1));
    return (h == 'S' || h == 'W') ? -x : x;
  }

  private static void putText(ObjectNode row, String name, String raw) {
    String v = raw.trim();
    if (v.isEmpty()) {
      row.putNull(name);
    } else {
      row.put(name, v);
    }
  }

  /** HURDAT2 marks a missing integer with -999 (older files -99). */
  private static void putInt(ObjectNode row, String name, String raw) {
    int v = Integer.parseInt(raw.trim());
    if (v <= -99) {
      row.putNull(name);
    } else {
      row.put(name, v);
    }
  }
}
