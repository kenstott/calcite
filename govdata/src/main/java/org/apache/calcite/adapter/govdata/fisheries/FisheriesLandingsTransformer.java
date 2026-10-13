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
package org.apache.calcite.adapter.govdata.fisheries;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Flattens the NOAA Fisheries FOSS landings response into rows.
 *
 * <p>Input: one page of
 * {@code https://apps-st.fisheries.noaa.gov/ods/foss/landings/?q={"year":YYYY}}, whose
 * {@code items} array carries one object per state x species x year x collection x source.
 * Pagination stops on a short page, so the page envelope ({@code hasMore}, {@code links}) is
 * dropped here and only {@code items} is read.
 *
 * <p>Derived columns, all from fields of the same response row: {@code state_abbr} from
 * {@code state_name}, {@code is_salmon}/{@code salmon_species} from {@code ts_afs_name}, and
 * {@code salmon_origin} from {@code region_name} (NOAA's own "Great Lakes" region marks
 * stocked, non-native Pacific/Atlantic salmon).
 */
public class FisheriesLandingsTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FisheriesLandingsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** NOAA's pseudo-state for landings made at sea; it has no state to join to. */
  private static final String PROCESS_AT_SEA = "PROCESS AT SEA";

  private static final String SALMON_PREFIX = "SALMON, ";
  private static final String GREAT_LAKES_REGION = "Great Lakes";

  private static final Map<String, String> STATE_ABBR = new HashMap<String, String>();

  static {
    String[][] pairs = {
        {"ALABAMA", "AL"}, {"ALASKA", "AK"}, {"AMERICAN SAMOA", "AS"}, {"ARIZONA", "AZ"},
        {"ARKANSAS", "AR"}, {"CALIFORNIA", "CA"}, {"COLORADO", "CO"}, {"CONNECTICUT", "CT"},
        {"DELAWARE", "DE"}, {"DISTRICT OF COLUMBIA", "DC"}, {"FLORIDA", "FL"},
        {"FLORIDA-EAST", "FL"}, {"FLORIDA-WEST", "FL"}, {"GEORGIA", "GA"}, {"GUAM", "GU"},
        {"HAWAII", "HI"}, {"IDAHO", "ID"}, {"ILLINOIS", "IL"}, {"INDIANA", "IN"},
        {"IOWA", "IA"}, {"KANSAS", "KS"}, {"KENTUCKY", "KY"}, {"LOUISIANA", "LA"},
        {"MAINE", "ME"}, {"MARYLAND", "MD"}, {"MASSACHUSETTS", "MA"}, {"MICHIGAN", "MI"},
        {"MINNESOTA", "MN"}, {"MISSISSIPPI", "MS"}, {"MISSOURI", "MO"}, {"MONTANA", "MT"},
        {"NEBRASKA", "NE"}, {"NEVADA", "NV"}, {"NEW HAMPSHIRE", "NH"}, {"NEW JERSEY", "NJ"},
        {"NEW MEXICO", "NM"}, {"NEW YORK", "NY"}, {"NORTH CAROLINA", "NC"},
        {"NORTH DAKOTA", "ND"}, {"NORTHERN MARIANA ISLANDS", "MP"}, {"OHIO", "OH"},
        {"OKLAHOMA", "OK"}, {"OREGON", "OR"}, {"PENNSYLVANIA", "PA"}, {"PUERTO RICO", "PR"},
        {"RHODE ISLAND", "RI"}, {"SOUTH CAROLINA", "SC"}, {"SOUTH DAKOTA", "SD"},
        {"TENNESSEE", "TN"}, {"TEXAS", "TX"}, {"UTAH", "UT"}, {"VERMONT", "VT"},
        {"VIRGIN ISLANDS", "VI"}, {"VIRGINIA", "VA"}, {"WASHINGTON", "WA"},
        {"WEST VIRGINIA", "WV"}, {"WISCONSIN", "WI"}, {"WYOMING", "WY"}
    };
    for (String[] p : pairs) {
      STATE_ABBR.put(p[0], p[1]);
    }
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException("FOSS landings: empty response for " + context.getUrl());
    }

    try {
      JsonNode items = MAPPER.readTree(response).get("items");
      if (items == null || !items.isArray()) {
        throw new IllegalStateException("FOSS landings: response has no 'items' array for "
            + context.getUrl());
      }

      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode item : items) {
        result.add(toRow(item));
      }
      return MAPPER.writeValueAsString(result);
    } catch (java.io.IOException e) {
      throw new IllegalStateException("FOSS landings: unparseable response for "
          + context.getUrl(), e);
    }
  }

  private ObjectNode toRow(JsonNode item) {
    ObjectNode row = MAPPER.createObjectNode();

    JsonNode year = item.get("year");
    if (year == null || !year.isNumber()) {
      throw new IllegalStateException("FOSS landings: row without numeric year: " + item);
    }
    row.put("landing_year", year.intValue());
    putText(row, "collection", item, "collection");
    putText(row, "source", item, "source");

    String stateName = text(item, "state_name");
    row.put("state_name", stateName);
    String abbr = stateName == null ? null : STATE_ABBR.get(stateName);
    if (abbr == null && !PROCESS_AT_SEA.equals(stateName)) {
      LOGGER.warn("FOSS landings: state_name '{}' has no state_abbr mapping", stateName);
    }
    row.put("state_abbr", abbr);
    row.put("is_state", abbr != null);

    putText(row, "region_name", item, "region_name");
    putText(row, "tsn", item, "tsn");
    putText(row, "species_name", item, "ts_afs_name");
    putText(row, "scientific_name", item, "ts_scientific_name");
    putNumber(row, "pounds", item, "pounds");
    putNumber(row, "dollars", item, "dollars");
    putNumber(row, "fish_count", item, "tot_count");

    String species = text(item, "ts_afs_name");
    boolean salmon = species != null && species.startsWith(SALMON_PREFIX);
    row.put("is_salmon", salmon);
    if (salmon) {
      String suffix = species.substring(SALMON_PREFIX.length()).replace("*", "").trim();
      row.put("salmon_species", suffix.toLowerCase());
      row.put("salmon_origin",
          GREAT_LAKES_REGION.equals(text(item, "region_name")) ? "great_lakes_stocked" : "coastal");
    } else {
      row.putNull("salmon_species");
      row.putNull("salmon_origin");
    }
    return row;
  }

  private static String text(JsonNode item, String field) {
    JsonNode n = item.get(field);
    return n == null || n.isNull() ? null : n.asText();
  }

  private static void putText(ObjectNode row, String out, JsonNode item, String field) {
    row.put(out, text(item, field));
  }

  private static void putNumber(ObjectNode row, String out, JsonNode item, String field) {
    JsonNode n = item.get(field);
    if (n == null || n.isNull()) {
      row.putNull(out);
    } else if (n.isNumber()) {
      row.put(out, n.doubleValue());
    } else {
      throw new IllegalStateException("FOSS landings: non-numeric " + field + ": " + n);
    }
  }
}
