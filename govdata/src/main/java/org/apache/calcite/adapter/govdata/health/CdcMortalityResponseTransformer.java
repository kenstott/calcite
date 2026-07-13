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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.PerRecordResponseTransformer;
import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

/**
 * Transforms CDC mortality data from two Socrata endpoints into a unified schema.
 *
 * <p>Handles two response shapes based on the "source_type" dimension:
 * <ul>
 *   <li>annual: NCHS Leading Causes of Death (bi63-dtpu) — source fields: year, state,
 *       cause_name (short leading-cause name), _113_cause_name (detailed ICD-10 cause),
 *       deaths, aadr (age-adjusted death rate)</li>
 *   <li>weekly: Weekly Provisional Counts of Deaths by State and Select Causes (muzy-jte6) —
 *       a wide table with one column per cause. We surface the COVID-19 underlying-cause count
 *       (covid_19_u071_underlying_cause_of_death) as a single COVID-19 row per state-week,
 *       sourcing mmwryear, week_ending_date, and jurisdiction_of_occurrence.</li>
 * </ul>
 * Both are normalised to: year, week_ending_date, state, cause_name, full_cause_name, deaths,
 * age_adjusted_rate. Weekly carries raw counts only, so age_adjusted_rate is null.
 *
 * <p>Implements {@link PerRecordResponseTransformer} so HttpSource's streamFromRawCache path
 * handles the paginated {@code {"results":[...]}} cache envelope correctly.
 */
public class CdcMortalityResponseTransformer implements PerRecordResponseTransformer {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode out = MAPPER.createArrayNode();

      // Socrata live response is a top-level array.
      if (!root.isArray()) {
        return "[]";
      }

      boolean isWeekly = isWeekly(context);

      for (JsonNode record : root) {
        ObjectNode row = MAPPER.createObjectNode();
        if (isWeekly) {
          mapWeekly(record, row);
        } else {
          mapAnnual(record, row);
        }
        out.add(row);
      }

      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to transform CDC mortality response", e);
    }
  }

  @Override
  public void transformRecord(Map<String, Object> row, RequestContext context) {
    Map<String, Object> source = new HashMap<>(row);
    row.clear();
    if (isWeekly(context)) {
      mapWeeklyMap(source, row);
    } else {
      mapAnnualMap(source, row);
    }
  }

  private static boolean isWeekly(RequestContext context) {
    return context != null && "weekly".equals(context.getDimensionValues().get("source_type"));
  }

  private void mapAnnual(JsonNode r, ObjectNode row) {
    put(row, "year", text(r, "year"));
    put(row, "week_ending_date", null);
    put(row, "state", text(r, "state"));
    put(row, "cause_name", text(r, "cause_name"));
    put(row, "full_cause_name", text(r, "_113_cause_name"));
    put(row, "deaths", text(r, "deaths"));
    put(row, "age_adjusted_rate", text(r, "aadr"));
    put(row, "source_type", "annual");
  }

  private void mapWeekly(JsonNode r, ObjectNode row) {
    put(row, "year", text(r, "mmwryear"));
    put(row, "week_ending_date", text(r, "week_ending_date"));
    put(row, "state", text(r, "jurisdiction_of_occurrence"));
    put(row, "cause_name", "COVID-19");
    put(row, "full_cause_name", "COVID-19 (underlying cause of death)");
    put(row, "deaths", text(r, "covid_19_u071_underlying_cause_of_death"));
    put(row, "age_adjusted_rate", null);
    put(row, "source_type", "weekly");
  }

  private void mapAnnualMap(Map<String, Object> r, Map<String, Object> row) {
    row.put("year", str(r.get("year")));
    row.put("week_ending_date", null);
    row.put("state", str(r.get("state")));
    row.put("cause_name", str(r.get("cause_name")));
    row.put("full_cause_name", str(r.get("_113_cause_name")));
    row.put("deaths", str(r.get("deaths")));
    row.put("age_adjusted_rate", str(r.get("aadr")));
    row.put("source_type", "annual");
  }

  private void mapWeeklyMap(Map<String, Object> r, Map<String, Object> row) {
    row.put("year", str(r.get("mmwryear")));
    row.put("week_ending_date", str(r.get("week_ending_date")));
    row.put("state", str(r.get("jurisdiction_of_occurrence")));
    row.put("cause_name", "COVID-19");
    row.put("full_cause_name", "COVID-19 (underlying cause of death)");
    row.put("deaths", str(r.get("covid_19_u071_underlying_cause_of_death")));
    row.put("age_adjusted_rate", null);
    row.put("source_type", "weekly");
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asText(null);
  }

  private static void put(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, value);
    }
  }

  private static String str(Object value) {
    return value == null ? null : String.valueOf(value);
  }
}
