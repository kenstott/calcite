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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Transforms NOAA CDO GHCND daily API responses from long format to wide format.
 *
 * <p>Input format (CDO v2 results endpoint, long format):
 * <pre>{@code
 * {"results": [{"date": "2020-01-01T00:00:00", "datatype": "TMAX",
 *               "station": "GHCND:USW00094846", "attributes": ",,S,", "value": -22.2}]}
 * }</pre>
 *
 * <p>Output: one row per (station_id, date) with element values pivoted to columns.
 *
 * <p>Unit note: CDO returns TMAX/TMIN/TAVG in tenths of degrees C, PRCP in tenths
 * of mm, AWND in tenths of m/s. This transformer divides those by 10 to store
 * values in standard units (°C, mm, m/s). SNOW and SNWD are returned in mm as-is.
 */
public class GhcndDailyTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GhcndDailyTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("GHCND Daily: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (!root.isObject()) {
        LOGGER.warn("GHCND Daily: Unexpected response type {} for {}",
            root.getNodeType(), context.getUrl());
        return "[]";
      }

      JsonNode results = root.get("results");
      if (results == null || !results.isArray()) {
        LOGGER.debug("GHCND Daily: No results in response for {}", context.getUrl());
        return "[]";
      }

      String stateFips = context.getDimensionValues().get("state_fips");

      // Keyed by "station_id|date" to preserve insertion order across stations and days.
      Map<String, StationDayRecord> groups = new LinkedHashMap<String, StationDayRecord>();

      for (JsonNode item : results) {
        String rawStation = getTextOrNull(item, "station");
        if (rawStation == null) {
          continue;
        }
        String stationId = rawStation.startsWith("GHCND:") ? rawStation.substring(6) : rawStation;

        String rawDate = getTextOrNull(item, "date");
        if (rawDate == null) {
          continue;
        }
        String date = rawDate.length() >= 10 ? rawDate.substring(0, 10) : rawDate;

        String key = stationId + "|" + date;
        StationDayRecord rec = groups.get(key);
        if (rec == null) {
          rec = new StationDayRecord(stationId, date);
          groups.put(key, rec);
        }

        String datatype = getTextOrNull(item, "datatype");
        if (datatype == null) {
          continue;
        }

        JsonNode valueNode = item.get("value");
        Double value = (valueNode != null && valueNode.isNumber()) ? valueNode.doubleValue() : null;
        String attributes = getTextOrNull(item, "attributes");

        // CDO returns TMAX/TMIN/TAVG in tenths of °C, PRCP in tenths of mm,
        // AWND in tenths of m/s. Divide by 10 to store in standard units.
        if ("TMAX".equals(datatype)) {
          rec.tmaxC = value != null ? value / 10.0 : null;
          rec.tmaxFlag = attributes;
        } else if ("TMIN".equals(datatype)) {
          rec.tminC = value != null ? value / 10.0 : null;
          rec.tminFlag = attributes;
        } else if ("TAVG".equals(datatype)) {
          rec.tavgC = value != null ? value / 10.0 : null;
        } else if ("PRCP".equals(datatype)) {
          rec.prcpMm = value != null ? value / 10.0 : null;
          rec.prcpFlag = attributes;
        } else if ("SNOW".equals(datatype)) {
          rec.snowMm = value;
        } else if ("SNWD".equals(datatype)) {
          rec.snwdMm = value;
        } else if ("AWND".equals(datatype)) {
          rec.awndMs = value != null ? value / 10.0 : null;
        }
      }

      ArrayNode output = MAPPER.createArrayNode();
      for (StationDayRecord rec : groups.values()) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("station_id", rec.stationId);
        row.put("date", rec.date);

        int year = -1;
        if (rec.date.length() >= 4) {
          try {
            year = Integer.parseInt(rec.date.substring(0, 4));
          } catch (NumberFormatException e) {
            // leave year as -1
          }
        }
        if (year >= 0) {
          row.put("year", year);
        } else {
          row.putNull("year");
        }

        row.put("state_fips", stateFips);

        putDoubleOrNull(row, "tmax_c", rec.tmaxC);
        putDoubleOrNull(row, "tmin_c", rec.tminC);
        putDoubleOrNull(row, "tavg_c", rec.tavgC);
        putDoubleOrNull(row, "prcp_mm", rec.prcpMm);
        putDoubleOrNull(row, "snow_mm", rec.snowMm);
        putDoubleOrNull(row, "snwd_mm", rec.snwdMm);
        putDoubleOrNull(row, "awnd_ms", rec.awndMs);

        putStringOrNull(row, "tmax_flag", rec.tmaxFlag);
        putStringOrNull(row, "tmin_flag", rec.tminFlag);
        putStringOrNull(row, "prcp_flag", rec.prcpFlag);

        output.add(row);
      }

      LOGGER.debug("GHCND Daily: Transformed {} station-day records for state_fips={}",
          output.size(), stateFips);
      return output.toString();

    } catch (Exception e) {
      LOGGER.error("GHCND Daily: Failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private static void putDoubleOrNull(ObjectNode row, String field, Double value) {
    if (value != null) {
      row.put(field, value);
    } else {
      row.putNull(field);
    }
  }

  private static void putStringOrNull(ObjectNode row, String field, String value) {
    if (value != null) {
      row.put(field, value);
    } else {
      row.putNull(field);
    }
  }

  private static String getTextOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asText();
  }

  private static final class StationDayRecord {
    final String stationId;
    final String date;

    Double tmaxC;
    Double tminC;
    Double tavgC;
    Double prcpMm;
    Double snowMm;
    Double snwdMm;
    Double awndMs;

    String tmaxFlag;
    String tminFlag;
    String prcpFlag;

    StationDayRecord(String stationId, String date) {
      this.stationId = stationId;
      this.date = date;
    }
  }
}
