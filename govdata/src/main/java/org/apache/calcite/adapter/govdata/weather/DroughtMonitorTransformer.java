/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Transforms USDM (US Drought Monitor) API JSON responses into flat JSON arrays
 * for the {@code drought_monitor_weekly} table.
 *
 * <p>Input format:
 * <pre>{@code
 * [
 *   {
 *     "MapDate": 20230328,
 *     "FIPS": "31001",
 *     "County": "Adams County",
 *     "State": "NE",
 *     "None": 0.00,
 *     "D0": 100.00,
 *     "D1": 100.00,
 *     "D2": 77.82,
 *     "D3": 0.00,
 *     "D4": 0.00,
 *     "ValidStart": "2023-03-28",
 *     "ValidEnd": "2023-04-03",
 *     "StatisticFormatID": 1
 *   }
 * ]
 * }</pre>
 *
 * <p>D0-D4 values are cumulative (D0 = area with at least D0 intensity, etc.).
 * Exclusive per-category percentages are derived by subtraction.
 * DSCI = D0 + D1 + D2 + D3 + D4 (sum of cumulative values, range 0-500).
 */
public class DroughtMonitorTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DroughtMonitorTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

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
      JsonNode root = MAPPER.readTree(response);

      ArrayNode array = resolveArray(root);
      if (array == null) {
        LOGGER.warn("Drought Monitor: No array found in response for {}", context.getUrl());
        return "[]";
      }

      String stateAbbrDim = context.getDimensionValues().get("state_abbr");
      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode item : array) {
        JsonNode fipsNode = item.get("FIPS");
        if (fipsNode == null || fipsNode.isNull()) {
          continue;
        }

        String rawFips = fipsNode.asText().trim();
        if (rawFips.isEmpty()) {
          continue;
        }

        String stateField = getTextOrNull(item, "State");
        String stateAbbr = stateField != null ? stateField : stateAbbrDim;
        String stateFips = stateAbbr != null ? STATE_FIPS.get(stateAbbr) : null;

        int mapDateInt = item.has("MapDate") ? item.get("MapDate").asInt(0) : 0;
        String weekDate = mapDateToString(mapDateInt);
        int year = Integer.parseInt(weekDate.substring(0, 4));

        double d0 = getDoubleOrZero(item, "D0");
        double d1 = getDoubleOrZero(item, "D1");
        double d2 = getDoubleOrZero(item, "D2");
        double d3 = getDoubleOrZero(item, "D3");
        double d4 = getDoubleOrZero(item, "D4");

        double d0Pct = Math.max(0.0, d0 - d1);
        double d1Pct = Math.max(0.0, d1 - d2);
        double d2Pct = Math.max(0.0, d2 - d3);
        double d3Pct = Math.max(0.0, d3 - d4);
        double d4Pct = Math.max(0.0, d4);
        double dsci = d0 + d1 + d2 + d3 + d4;

        ObjectNode row = MAPPER.createObjectNode();
        row.put("county_fips", String.format("%05d", Integer.parseInt(rawFips)));
        row.put("state_abbr", stateAbbr);
        if (stateFips != null) {
          row.put("state_fips", stateFips);
        } else {
          row.putNull("state_fips");
        }
        row.put("county_name", getTextOrNull(item, "County"));
        row.put("week_date", weekDate);
        row.put("year", year);
        row.put("valid_start", getTextOrNull(item, "ValidStart"));
        row.put("valid_end", getTextOrNull(item, "ValidEnd"));
        row.put("none_pct", getDoubleOrZero(item, "None"));
        row.put("d0_pct", d0Pct);
        row.put("d1_pct", d1Pct);
        row.put("d2_pct", d2Pct);
        row.put("d3_pct", d3Pct);
        row.put("d4_pct", d4Pct);
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

  private static ArrayNode resolveArray(JsonNode root) {
    if (root.isArray()) {
      return (ArrayNode) root;
    }
    if (root.isObject()) {
      for (String candidate : new String[]{"data", "results", "features", "items"}) {
        JsonNode child = root.get(candidate);
        if (child != null && child.isArray()) {
          return (ArrayNode) child;
        }
      }
    }
    return null;
  }

  private static String mapDateToString(int mapDate) {
    int y = mapDate / 10000;
    int m = (mapDate / 100) % 100;
    int d = mapDate % 100;
    return y + "-" + pad2(m) + "-" + pad2(d);
  }

  private static String pad2(int v) {
    return v < 10 ? "0" + v : Integer.toString(v);
  }

  private static double getDoubleOrZero(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return 0.0;
    }
    return v.asDouble(0.0);
  }

  private static String getTextOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    return v.asText();
  }
}
