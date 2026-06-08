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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Transforms USDM (US Drought Monitor) API CSV responses into flat JSON arrays
 * for the {@code drought_monitor_weekly} table.
 *
 * <p>The USDM API returns CSV (not JSON). Header row:
 * MapDate,FIPS,County,State,None,D0,D1,D2,D3,D4,ValidStart,ValidEnd,StatisticFormatID
 *
 * <p>D0-D4 values are cumulative (D0 = area with at least D0 intensity, etc.).
 * Exclusive per-category percentages are derived by subtraction.
 * DSCI = D0 + D1 + D2 + D3 + D4 (sum of cumulative values, range 0-500).
 */
public class DroughtMonitorTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DroughtMonitorTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Column indices in the USDM CSV
  private static final int COL_MAP_DATE = 0;
  private static final int COL_FIPS = 1;
  private static final int COL_COUNTY = 2;
  private static final int COL_STATE = 3;
  private static final int COL_NONE = 4;
  private static final int COL_D0 = 5;
  private static final int COL_D1 = 6;
  private static final int COL_D2 = 7;
  private static final int COL_D3 = 8;
  private static final int COL_D4 = 9;
  private static final int COL_VALID_START = 10;
  private static final int COL_VALID_END = 11;

  private static final Map<String, String> STATE_FIPS;
  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("AL", "01"); m.put("AK", "02"); m.put("AZ", "04"); m.put("AR", "05");
    m.put("CA", "06"); m.put("CO", "08"); m.put("CT", "09"); m.put("DE", "10");
    m.put("DC", "11"); m.put("FL", "12"); m.put("GA", "13"); m.put("HI", "15");
    m.put("ID", "16"); m.put("IL", "17"); m.put("IN", "18"); m.put("IA", "19");
    m.put("KS", "20"); m.put("KY", "21"); m.put("LA", "22"); m.put("ME", "23");
    m.put("MD", "24"); m.put("MA", "25"); m.put("MI", "26"); m.put("MN", "27");
    m.put("MS", "28"); m.put("MO", "29"); m.put("MT", "30"); m.put("NE", "31");
    m.put("NV", "32"); m.put("NH", "33"); m.put("NJ", "34"); m.put("NM", "35");
    m.put("NY", "36"); m.put("NC", "37"); m.put("ND", "38"); m.put("OH", "39");
    m.put("OK", "40"); m.put("OR", "41"); m.put("PA", "42"); m.put("RI", "44");
    m.put("SC", "45"); m.put("SD", "46"); m.put("TN", "47"); m.put("TX", "48");
    m.put("UT", "49"); m.put("VT", "50"); m.put("VA", "51"); m.put("WA", "53");
    m.put("WV", "54"); m.put("WI", "55"); m.put("WY", "56");
    STATE_FIPS = Collections.unmodifiableMap(m);
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Drought Monitor: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      String stateAbbrDim = context.getDimensionValues().get("state_abbr");
      ArrayNode result = MAPPER.createArrayNode();
      String[] lines = response.split("\n");
      boolean firstLine = true;

      for (String line : lines) {
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }
        // Skip header row
        if (firstLine) {
          firstLine = false;
          if (line.startsWith("MapDate")) {
            continue;
          }
        }

        String[] cols = line.split(",", -1);
        if (cols.length < 12) {
          continue;
        }

        String rawFips = cols[COL_FIPS].trim();
        if (rawFips.isEmpty() || rawFips.equals("null")) {
          continue;
        }

        String stateAbbr = cols[COL_STATE].trim();
        if (stateAbbr.isEmpty()) {
          stateAbbr = stateAbbrDim;
        }
        String stateFips = stateAbbr != null ? STATE_FIPS.get(stateAbbr) : null;

        String mapDateStr = cols[COL_MAP_DATE].trim();
        String weekDate = mapDateStr.length() == 8 ? mapDateToString(mapDateStr) : mapDateStr;
        int year = weekDate.length() >= 4
            ? Integer.parseInt(weekDate.substring(0, 4)) : 0;

        double noneVal = parseDouble(cols[COL_NONE]);
        double d0 = parseDouble(cols[COL_D0]);
        double d1 = parseDouble(cols[COL_D1]);
        double d2 = parseDouble(cols[COL_D2]);
        double d3 = parseDouble(cols[COL_D3]);
        double d4 = parseDouble(cols[COL_D4]);

        double d0Pct = Math.max(0.0, d0 - d1);
        double d1Pct = Math.max(0.0, d1 - d2);
        double d2Pct = Math.max(0.0, d2 - d3);
        double d3Pct = Math.max(0.0, d3 - d4);
        double dsci = d0 + d1 + d2 + d3 + d4;

        ObjectNode row = MAPPER.createObjectNode();
        try {
          row.put("county_fips", String.format("%05d", Integer.parseInt(rawFips)));
        } catch (NumberFormatException e) {
          row.put("county_fips", rawFips);
        }
        row.put("state_abbr", stateAbbr);
        if (stateFips != null) {
          row.put("state_fips", stateFips);
        } else {
          row.putNull("state_fips");
        }
        row.put("county_name", cols[COL_COUNTY].trim());
        row.put("week_date", weekDate);
        row.put("year", year);
        row.put("valid_start", cols[COL_VALID_START].trim());
        row.put("valid_end", cols[COL_VALID_END].trim());
        row.put("none_pct", noneVal);
        row.put("d0_pct", d0Pct);
        row.put("d1_pct", d1Pct);
        row.put("d2_pct", d2Pct);
        row.put("d3_pct", d3Pct);
        row.put("d4_pct", Math.max(0.0, d4));
        row.put("dsci", dsci);

        result.add(row);
      }

      LOGGER.debug("Drought Monitor: Transformed {} records for state_abbr={}",
          result.size(), stateAbbrDim);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("Drought Monitor: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private static String mapDateToString(String mapDate) {
    // "20230328" → "2023-03-28"
    return mapDate.substring(0, 4) + "-" + mapDate.substring(4, 6) + "-" + mapDate.substring(6, 8);
  }

  private static double parseDouble(String s) {
    if (s == null || s.trim().isEmpty()) {
      return 0.0;
    }
    try {
      return Double.parseDouble(s.trim());
    } catch (NumberFormatException e) {
      return 0.0;
    }
  }
}
