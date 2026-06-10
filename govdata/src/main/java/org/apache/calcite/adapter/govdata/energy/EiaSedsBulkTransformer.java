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
package org.apache.calcite.adapter.govdata.energy;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Streams the EIA bulk {@code SEDS.zip} and materializes annual State Energy Data System rows —
 * the bulk drop-in for the per-year {@code eia_state_energy_consumption} EIA API v2 fan-out.
 *
 * <p>Each annual series is {@code SEDS.{MSN}.{state}.A} with {@code data:[[YYYY,value],...]}.
 * The MSN encodes fuel (chars 0-1) and sector (chars 2-3); the value is classified into
 * consumption/expenditure/price by the units, mirroring the existing API transformer.
 */
public class EiaSedsBulkTransformer extends EiaBulkSeriesTransformer {

  private static final Pattern SEDS_SERIES = Pattern.compile("^SEDS\\.[A-Z0-9]+\\.[A-Z]{2}\\.A$");

  @Override protected List<Map<String, Object>> rowsForSeries(JsonNode series) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    JsonNode sidNode = series.get("series_id");
    if (sidNode == null || !SEDS_SERIES.matcher(sidNode.asText()).matches()) {
      return rows;
    }
    String seriesId = sidNode.asText();
    String[] parts = seriesId.split("\\.");        // [SEDS, MSN, STATE, A]
    String msn = parts[1];
    String stateAbbr = parts[2];
    String units = series.path("units").asText(null);
    String name = series.path("name").asText(null);
    String stateName = stateFromName(name);
    String sector = deriveSector(msn);
    String fuel = deriveFuel(msn);

    JsonNode data = series.get("data");
    if (data == null || !data.isArray()) {
      return rows;
    }
    for (JsonNode point : data) {
      if (!point.isArray() || point.size() < 2 || point.get(1).isNull()) {
        continue;
      }
      int year;
      try {
        year = Integer.parseInt(point.get(0).asText());
      } catch (NumberFormatException e) {
        continue;
      }
      double value = point.get(1).asDouble();
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("year", year);                       // partition column
      row.put("consumption_year", year);
      row.put("state_abbr", stateAbbr);
      row.put("state_name", stateName);
      row.put("msn", msn);
      row.put("sector", sector);
      row.put("fuel_type", fuel);
      row.put("value", value);
      row.put("units", units);
      // Classify into the metric column by units. Check "per" before "btu": the price unit
      // "Dollars per Million Btu" contains both, but is a price.
      String u = units == null ? "" : units.toLowerCase();
      row.put("consumption_bbtu", u.contains("per") ? null : (u.contains("btu") ? value : null));
      row.put("expenditure_million",
          !u.contains("per") && u.contains("dollar") ? value : null);
      row.put("price_per_mmbtu", u.contains("per") ? value : null);
      row.put("series_description", name);
      rows.add(row);
    }
    return rows;
  }

  /** "Total energy consumption, California" -> "California". */
  private static String stateFromName(String name) {
    if (name == null) {
      return null;
    }
    int comma = name.lastIndexOf(", ");
    return comma >= 0 ? name.substring(comma + 2) : null;
  }

  // --- MSN decode (mirrors EiaSEDSTransformer; that API transformer is retired by this) ---

  private static String deriveSector(String msn) {
    if (msn == null || msn.length() < 4) {
      return "Unknown";
    }
    String code = msn.substring(2, 4).toUpperCase();
    if ("RC".equals(code)) {
      return "Residential";
    } else if ("CC".equals(code)) {
      return "Commercial";
    } else if ("IC".equals(code)) {
      return "Industrial";
    } else if ("TC".equals(code)) {
      return "Transportation";
    } else if ("EI".equals(code)) {
      return "Electric Power";
    } else if ("AC".equals(code)) {
      return "Total";
    }
    return code;
  }

  private static String deriveFuel(String msn) {
    if (msn == null || msn.length() < 2) {
      return "Unknown";
    }
    String code = msn.substring(0, 2).toUpperCase();
    if ("CC".equals(code) || "CL".equals(code)) {
      return "Coal";
    } else if ("MG".equals(code) || "PQ".equals(code)) {
      return "Petroleum";
    } else if ("NG".equals(code)) {
      return "Natural Gas";
    } else if ("NU".equals(code)) {
      return "Nuclear";
    } else if ("HY".equals(code)) {
      return "Hydroelectric";
    } else if ("GE".equals(code)) {
      return "Geothermal";
    } else if ("WY".equals(code)) {
      return "Wind";
    } else if ("SO".equals(code)) {
      return "Solar";
    } else if ("WD".equals(code) || "LO".equals(code)) {
      return "Wood/Biomass";
    } else if ("TE".equals(code)) {
      return "Total";
    }
    return code;
  }

  /** Package-visible for unit tests. */
  static List<Map<String, Object>> rowsForSeriesStatic(JsonNode series) {
    return new EiaSedsBulkTransformer().rowsForSeries(series);
  }
}
