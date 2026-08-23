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
 * Transforms CDC mortality data from three Socrata endpoints into a unified schema.
 *
 * <p>Handles three response shapes based on the "source_type" dimension, which now carries
 * three fetch-routing values (annual, weekly_precovid, weekly_covid) — one per URL, so each
 * source is fetched exactly once per run instead of being iterated by year:
 * <ul>
 *   <li>annual: NCHS Leading Causes of Death (bi63-dtpu, 1999-2017) — source fields: year,
 *       state, cause_name (short leading-cause name), _113_cause_name (detailed ICD-10 cause),
 *       deaths, aadr (age-adjusted death rate)</li>
 *   <li>weekly, 2018-2019 vintage (3yf8-kanr) — a wide table with one column per select
 *       cause and no COVID columns (the series predates COVID-19). Field names in this
 *       vintage are unspaced (allcause, weekendingdate), unlike the newer vintage below.
 *       We surface the all-cause weekly total (allcause) as a single "All Cause" row per
 *       state-week, sourcing mmwryear, weekendingdate, and jurisdiction_of_occurrence.</li>
 *   <li>weekly, 2020-2023 vintage (muzy-jte6) — the same wide-table shape as 3yf8-kanr but
 *       with underscored field names (all_cause, week_ending_date) and added COVID-19
 *       columns. We surface the COVID-19 underlying-cause count
 *       (covid_19_u071_underlying_cause_of_death) as a single COVID-19 row per state-week,
 *       sourcing mmwryear, week_ending_date, and jurisdiction_of_occurrence.</li>
 * </ul>
 * All three are normalised to: year, week_ending_date, state, cause_name, full_cause_name,
 * deaths, age_adjusted_rate. Weekly carries raw counts only, so age_adjusted_rate is null.
 *
 * <p>Implements {@link PerRecordResponseTransformer} so HttpSource's streamFromRawCache path
 * handles the paginated {@code {"results":[...]}} cache envelope correctly.
 */
public class CdcMortalityResponseTransformer implements PerRecordResponseTransformer {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PRE_COVID_WEEKLY_DATASET = "3yf8-kanr";

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
      boolean isPreCovidWeekly = isWeekly && isPreCovidWeekly(context);

      for (JsonNode record : root) {
        ObjectNode row = MAPPER.createObjectNode();
        if (isPreCovidWeekly) {
          mapPreCovidWeekly(record, row);
        } else if (isWeekly) {
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
    if (isWeekly(context) && isPreCovidWeekly(context)) {
      mapPreCovidWeeklyMap(source, row);
    } else if (isWeekly(context)) {
      mapWeeklyMap(source, row);
    } else {
      mapAnnualMap(source, row);
    }
  }

  private static boolean isWeekly(RequestContext context) {
    if (context == null) {
      return false;
    }
    String sourceType = context.getDimensionValues().get("source_type");
    return "weekly_precovid".equals(sourceType) || "weekly_covid".equals(sourceType);
  }

  /**
   * Distinguishes the 2018-2019 (3yf8-kanr) weekly vintage from the 2020-2023 (muzy-jte6)
   * vintage. The source_type dimension carries this distinction directly
   * (weekly_precovid/weekly_covid); the request URL is checked too as a redundant signal
   * in case dimension values and urlRules ever drift apart.
   */
  private static boolean isPreCovidWeekly(RequestContext context) {
    if (context == null) {
      return false;
    }
    if ("weekly_precovid".equals(context.getDimensionValues().get("source_type"))) {
      return true;
    }
    return context.getUrl() != null && context.getUrl().contains(PRE_COVID_WEEKLY_DATASET);
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

  private void mapPreCovidWeekly(JsonNode r, ObjectNode row) {
    put(row, "year", text(r, "mmwryear"));
    put(row, "week_ending_date", text(r, "weekendingdate"));
    put(row, "state", text(r, "jurisdiction_of_occurrence"));
    put(row, "cause_name", "All Cause");
    put(row, "full_cause_name", "All Cause (weekly provisional)");
    put(row, "deaths", text(r, "allcause"));
    put(row, "age_adjusted_rate", null);
    put(row, "source_type", "weekly");
  }

  private void mapPreCovidWeeklyMap(Map<String, Object> r, Map<String, Object> row) {
    row.put("year", str(r.get("mmwryear")));
    row.put("week_ending_date", str(r.get("weekendingdate")));
    row.put("state", str(r.get("jurisdiction_of_occurrence")));
    row.put("cause_name", "All Cause");
    row.put("full_cause_name", "All Cause (weekly provisional)");
    row.put("deaths", str(r.get("allcause")));
    row.put("age_adjusted_rate", null);
    row.put("source_type", "weekly");
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
