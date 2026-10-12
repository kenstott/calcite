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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Transforms NOAA CO-OPS (Tides and Currents) responses into flat JSON rows. One class serves four
 * tables, selected by the {@code type} dimension:
 *
 * <ul>
 *   <li>{@code coops_station}: Metadata API station list ({@code stations.json?type=waterlevels}),
 *       restricted to tidal stations.</li>
 *   <li>{@code coops_station_datums}: Metadata API {@code datums.json} for one station, flattened
 *       to one row of datum elevations.</li>
 *   <li>{@code coops_high_low}: Data API {@code product=high_low} (observed extremes).</li>
 *   <li>{@code coops_tide_predictions}: Data API {@code product=predictions&interval=hilo}
 *       (predicted extremes).</li>
 * </ul>
 *
 * <p>The Data API signals a station/year with no data as an {@code error} object whose message
 * starts with "No data was found"; that is a legitimate empty result. Every other {@code error}
 * object is a request failure and throws.
 */
public class CoopsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String NO_DATA = "No data was found";

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
    m.put("PR", "72"); m.put("VI", "78");
    STATE_FIPS = Collections.unmodifiableMap(m);
  }

  /** CO-OPS publishes American Samoa as a spelled-out state value. */
  private static final String AMERICAN_SAMOA = "American Samoa";

  @Override public String transform(String response, RequestContext context) {
    String type = context.getDimensionValues().get("type");
    JsonNode root;
    try {
      root = MAPPER.readTree(response);
    } catch (IOException e) {
      throw new IllegalStateException("CO-OPS " + type + " response is not JSON: "
          + response.substring(0, Math.min(200, response.length())), e);
    }
    if (type == null) {
      throw new IllegalStateException("CO-OPS transformer requires the 'type' dimension");
    }
    JsonNode error = root.get("error");
    if (error != null) {
      String message = error.path("message").asText();
      if (message.contains(NO_DATA)) {
        return MAPPER.createArrayNode().toString();
      }
      throw new IllegalStateException("CO-OPS " + type + " request failed: " + message);
    }
    switch (type) {
    case "coops_station":
      return stations(root);
    case "coops_station_datums":
      return datums(root, context.getDimensionValues().get("station_id"));
    case "coops_high_low":
      return extremes(root.get("data"), context, true);
    case "coops_tide_predictions":
      return extremes(root.get("predictions"), context, false);
    default:
      throw new IllegalArgumentException("Unknown CO-OPS type dimension value: " + type);
    }
  }

  private static String stations(JsonNode root) {
    ArrayNode result = MAPPER.createArrayNode();
    for (JsonNode s : root.get("stations")) {
      if (!s.path("tidal").asBoolean()) {
        continue;
      }
      String state = s.path("state").asText();
      ObjectNode row = MAPPER.createObjectNode();
      row.put("station_id", s.get("id").asText());
      row.put("station_name", s.get("name").asText());
      row.put("state", state);
      String fips = AMERICAN_SAMOA.equals(state) ? "60" : STATE_FIPS.get(state);
      if (STATE_FIPS.containsKey(state)) {
        row.put("state_abbr", state);
      } else if (AMERICAN_SAMOA.equals(state)) {
        row.put("state_abbr", "AS");
      } else {
        row.putNull("state_abbr");
      }
      if (fips == null) {
        row.putNull("state_fips");
      } else {
        row.put("state_fips", fips);
      }
      row.put("latitude", s.get("lat").asDouble());
      row.put("longitude", s.get("lng").asDouble());
      putTextOrNull(row, "affiliations", s.get("affiliations"));
      putTextOrNull(row, "tide_type", s.get("tideType"));
      putTextOrNull(row, "timezone", s.get("timezone"));
      row.put("timezone_offset_hours", s.get("timezonecorr").asInt());
      row.put("observes_dst", s.path("observedst").asBoolean());
      row.put("storm_surge_forecast", s.path("stormsurge").asBoolean());
      result.add(row);
    }
    return result.toString();
  }

  private static String datums(JsonNode root, String station) {
    ObjectNode row = MAPPER.createObjectNode();
    row.put("station_id", station);
    putTextOrNull(row, "epoch", root.get("epoch"));
    putTextOrNull(row, "orthometric_datum", root.get("OrthometricDatum"));
    putTextOrNull(row, "datums_accepted", root.get("accepted"));
    Map<String, Double> byName = new HashMap<String, Double>();
    for (JsonNode d : root.path("datums")) {
      byName.put(d.get("name").asText(), d.get("value").asDouble());
    }
    putDatum(row, "stnd_m", byName, "STND");
    putDatum(row, "mhhw_m", byName, "MHHW");
    putDatum(row, "mhw_m", byName, "MHW");
    putDatum(row, "mtl_m", byName, "MTL");
    putDatum(row, "msl_m", byName, "MSL");
    putDatum(row, "mlw_m", byName, "MLW");
    putDatum(row, "mllw_m", byName, "MLLW");
    putDatum(row, "navd88_m", byName, "NAVD88");
    putDatum(row, "lat_m", byName, "LAT");
    putDatum(row, "hat_m", byName, "HAT");
    ArrayNode result = MAPPER.createArrayNode();
    result.add(row);
    return result.toString();
  }

  private static void putDatum(ObjectNode row, String field, Map<String, Double> byName,
      String name) {
    Double v = byName.get(name);
    if (v == null) {
      row.putNull(field);
    } else {
      row.put(field, v);
    }
  }

  private static String extremes(JsonNode data, RequestContext context, boolean observed) {
    String station = context.getDimensionValues().get("station_id");
    int year = Integer.parseInt(context.getDimensionValues().get("year"));
    if (data == null) {
      throw new IllegalStateException("CO-OPS response for station " + station + " year " + year
          + " has no data/predictions array");
    }
    ArrayNode result = MAPPER.createArrayNode();
    for (JsonNode e : data) {
      String t = e.get("t").asText();
      int rowYear = Integer.parseInt(t.substring(0, 4));
      if (rowYear != year) {
        throw new IllegalStateException("CO-OPS station " + station + " returned event " + t
            + " outside requested year " + year);
      }
      ObjectNode row = MAPPER.createObjectNode();
      row.put("station_id", station);
      row.put("event_time_utc", t);
      row.put("event_date", t.substring(0, 10));
      row.put("year", year);
      String v = e.path("v").asText();
      if (v.isEmpty()) {
        row.putNull("water_level_m");
      } else {
        row.put("water_level_m", Double.parseDouble(v));
      }
      if (observed) {
        row.put("event_type", e.get("ty").asText().trim());
        row.put("quality_flags", e.path("f").asText());
      } else {
        row.put("event_type", e.get("type").asText().trim());
      }
      result.add(row);
    }
    return result.toString();
  }

  private static void putTextOrNull(ObjectNode row, String field, JsonNode value) {
    if (value == null || value.isNull() || value.asText().isEmpty()) {
      row.putNull(field);
    } else {
      row.put(field, value.asText());
    }
  }
}
