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

import java.util.HashMap;
import java.util.Map;

/**
 * Flattens the NIFC WFIGS Incident Locations ArcGIS statistics response (one request per
 * discovery year, grouped server-side by {@code POOState, IncidentTypeCategory}) into
 * {@code wildfire_occurrence_by_state_year} rows.
 *
 * <p>{@code POOState} arrives as an ISO 3166-2 style code ({@code US-NH}, {@code CA-ON},
 * {@code MX-CH}); the country is the prefix and the state/province code the suffix. A
 * response flagged {@code exceededTransferLimit} is an error: the table is only correct when the
 * server returns every group in one page.
 */
public class WfigsIncidentStateYearTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String[] STATE_FIPS_PAIRS = {
    "AL", "01", "AK", "02", "AZ", "04", "AR", "05", "CA", "06", "CO", "08", "CT", "09",
    "DE", "10", "DC", "11", "FL", "12", "GA", "13", "HI", "15", "ID", "16", "IL", "17",
    "IN", "18", "IA", "19", "KS", "20", "KY", "21", "LA", "22", "ME", "23", "MD", "24",
    "MA", "25", "MI", "26", "MN", "27", "MS", "28", "MO", "29", "MT", "30", "NE", "31",
    "NV", "32", "NH", "33", "NJ", "34", "NM", "35", "NY", "36", "NC", "37", "ND", "38",
    "OH", "39", "OK", "40", "OR", "41", "PA", "42", "RI", "44", "SC", "45", "SD", "46",
    "TN", "47", "TX", "48", "UT", "49", "VT", "50", "VA", "51", "WA", "53", "WV", "54",
    "WI", "55", "WY", "56", "AS", "60", "GU", "66", "MP", "69", "PR", "72", "VI", "78"
  };

  private static final Map<String, String> STATE_FIPS = new HashMap<>();
  static {
    for (int i = 0; i < STATE_FIPS_PAIRS.length; i += 2) {
      STATE_FIPS.put(STATE_FIPS_PAIRS[i], STATE_FIPS_PAIRS[i + 1]);
    }
  }

  @Override public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode error = root.path("error");
      if (!error.isMissingNode() && !error.isNull()) {
        throw new RuntimeException("WFIGS incident statistics: ArcGIS error: " + error);
      }
      if (root.path("exceededTransferLimit").asBoolean(false)) {
        throw new RuntimeException("WFIGS incident statistics: response exceeded the server "
            + "transfer limit; groups would be missing");
      }
      JsonNode features = root.path("features");
      if (!features.isArray()) {
        throw new RuntimeException("WFIGS incident statistics: no 'features' array in response: "
            + response.substring(0, Math.min(200, response.length())));
      }
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode feature : features) {
        JsonNode a = feature.path("attributes");
        String poo = a.path("POOState").asText("");
        int dash = poo.indexOf('-');
        if (dash != 2 || poo.length() < 4) {
          throw new RuntimeException("WFIGS incident statistics: unexpected POOState '" + poo + "'");
        }
        String country = poo.substring(0, 2);
        String region = poo.substring(dash + 1);
        ObjectNode row = MAPPER.createObjectNode();
        row.put("country", country);
        row.put("state_abbr", region);
        String fips = "US".equals(country) ? STATE_FIPS.get(region) : null;
        if ("US".equals(country) && fips == null) {
          throw new RuntimeException("WFIGS incident statistics: unknown US state code '"
              + region + "'");
        }
        if (fips != null) {
          row.put("state_fips", fips);
        } else {
          row.putNull("state_fips");
        }
        row.put("incident_type_category", a.path("IncidentTypeCategory").asText());
        row.put("fire_count", a.path("fire_count").asInt());
        putDouble(row, "incident_acres", a.path("incident_acres"));
        putDouble(row, "final_acres", a.path("final_acres"));
        out.add(row);
      }
      return MAPPER.writeValueAsString(out);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("WFIGS incident statistics transform failed: " + e.getMessage(), e);
    }
  }

  private static void putDouble(ObjectNode row, String name, JsonNode v) {
    if (v.isNull() || v.isMissingNode()) {
      row.putNull(name);
    } else {
      row.put(name, v.asDouble());
    }
  }
}
