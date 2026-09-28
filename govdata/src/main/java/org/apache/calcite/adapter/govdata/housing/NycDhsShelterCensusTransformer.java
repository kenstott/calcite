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
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Maps NYC Open Data DHS Daily Shelter Census responses into
 * {@code homeless_shelter_census} rows. The {@code census_period} dimension fetches
 * two distinct Socrata resources — {@code k46n-sa2m} ("current") and {@code dwrg-kzni}
 * ("historical", back to 2013-08-21) — which the source publishes as separate datasets
 * rather than one continuous series. Each resource's own metadata description is
 * wrong about where the split falls: {@code k46n-sa2m} claims to start 2021-01-03 and
 * {@code dwrg-kzni} claims to run "prior to 3/1/2021", which read as an ~8-week
 * overlap — but querying both live (2026-09-28) shows {@code k46n-sa2m}'s actual
 * earliest row is 2021-03-01 and {@code dwrg-kzni}'s actual latest row is 2021-02-28:
 * a clean, non-overlapping boundary, not an overlap. {@link #CURRENT_RESOURCE_START}
 * is that confirmed boundary, not the resource's own (wrong) documented one; trusting
 * the documented 2021-01-03 date here previously dropped eight weeks of real
 * historical data that {@code k46n-sa2m} never actually covers (caught via DQ:
 * T2_row_count fell from the expected ~4,664 to 2,035 — exactly k46n-sa2m's own count,
 * meaning the transformer had silently filtered out the entire dwrg-kzni contribution).
 * The two resources also use slightly different field names for the same twelve
 * metrics (historical truncates several names); this transformer normalizes both
 * field-name sets to one canonical column set.
 */
public class NycDhsShelterCensusTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Confirmed live 2026-09-28 as k46n-sa2m's actual earliest row / dwrg-kzni's actual
   * latest row + 1 day — NOT either resource's own (wrong) documented boundary. */
  private static final String CURRENT_RESOURCE_START = "2021-03-01";

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    boolean historical = "dwrg-kzni".equals(context.getDimensionValues().get("census_period"));
    try {
      JsonNode root = MAPPER.readTree(response);
      if (!root.isArray()) {
        return "[]";
      }
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode rec : root) {
        String date = dateOnly(text(rec, "date_of_census"));
        if (date == null) {
          continue;
        }
        if (historical && date.compareTo(CURRENT_RESOURCE_START) >= 0) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("date_of_census", date);
        putInt(row, "total_adults_in_shelter", rec, "total_adults_in_shelter");
        putInt(row, "total_children_in_shelter", rec, "total_children_in_shelter");
        putInt(row, "total_individuals_in_shelter", rec, "total_individuals_in_shelter");
        putInt(row, "single_adult_men_in_shelter", rec, "single_adult_men_in_shelter");
        putInt(row, "single_adult_women_in_shelter", rec, "single_adult_women_in_shelter");
        putInt(row, "total_single_adults_in_shelter", rec, "total_single_adults_in_shelter");
        putInt(row, "families_with_children_in_shelter", rec,
            historical ? "families_with_children_in" : "families_with_children_in_shelter");
        putInt(row, "adults_in_families_with_children_in_shelter", rec,
            historical ? "adults_in_families_with" : "adults_in_families_with_children_in_shelter");
        putInt(row, "children_in_families_with_children_in_shelter", rec,
            historical ? "children_in_families_with" : "children_in_families_with_children_in_shelter");
        putInt(row, "total_individuals_in_families_with_children_in_shelter", rec,
            historical ? "total_individuals_in_families"
                : "total_individuals_in_families_with_children_in_shelter_");
        putInt(row, "adult_families_in_shelter", rec, "adult_families_in_shelter");
        putInt(row, "individuals_in_adult_families_in_shelter", rec,
            historical ? "individuals_in_adult_families" : "individuals_in_adult_families_in_shelter");
        out.add(row);
      }
      return MAPPER.writeValueAsString(out);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("NycDhsShelterCensusTransformer transform failed: "
          + e.getMessage(), e);
    }
  }

  /** Truncates a Socrata {@code calendar_date} value ({@code 2026-09-25T00:00:00.000}) to
   * a plain {@code YYYY-MM-DD} string, or null when absent. */
  private static String dateOnly(String raw) {
    return raw != null && raw.length() >= 10 ? raw.substring(0, 10) : null;
  }

  private static String text(JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    return v.isMissingNode() || v.isNull() ? null : v.asText();
  }

  /** Puts an integer value, tolerating numeric-in-string (Socrata's {@code number} type
   * serializes as a JSON string); null when absent/blank/non-numeric. */
  private static void putInt(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
      return;
    }
    if (v.isNumber()) {
      row.put(col, v.asInt());
      return;
    }
    String s = v.asText().trim();
    if (s.isEmpty()) {
      row.putNull(col);
      return;
    }
    try {
      row.put(col, (int) Double.parseDouble(s));
    } catch (NumberFormatException e) {
      row.putNull(col);
    }
  }
}
