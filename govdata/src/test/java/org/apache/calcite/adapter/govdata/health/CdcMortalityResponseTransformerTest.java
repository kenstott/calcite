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

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the muzy-jte6 weekly fan-out (one All Cause row plus one COVID-19 row per state-week)
 * and the suppression-flag handling, using records shaped like the live Socrata responses.
 */
@Tag("unit")
class CdcMortalityResponseTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final CdcMortalityResponseTransformer transformer = new CdcMortalityResponseTransformer();

  private static RequestContext ctx(String sourceType) {
    return RequestContext.builder()
        .url("https://data.cdc.gov/resource/muzy-jte6.json")
        .dimensionValues(Collections.singletonMap("source_type", sourceType))
        .build();
  }

  private static Map<String, Object> record(String allCause, String covidUcod,
      String flagMcod, String flagUcod) {
    Map<String, Object> r = new HashMap<>();
    r.put("jurisdiction_of_occurrence", "Alabama");
    r.put("mmwryear", "2020");
    r.put("week_ending_date", "2020-02-29");
    r.put("all_cause", allCause);
    if (covidUcod != null) {
      r.put("covid_19_u071_underlying_cause_of_death", covidUcod);
    }
    if (flagMcod != null) {
      r.put("flag_cov19mcod", flagMcod);
    }
    if (flagUcod != null) {
      r.put("flag_cov19ucod", flagUcod);
    }
    return r;
  }

  private static Map<String, Object> byCause(List<Map<String, Object>> rows, String cause) {
    for (Map<String, Object> row : rows) {
      if (cause.equals(row.get("cause_name"))) {
        return row;
      }
    }
    throw new AssertionError("no " + cause + " row in " + rows);
  }

  @Test void covidVintageFansOutToAllCauseAndCovidRows() {
    List<Map<String, Object>> rows =
        transformer.transformRecordToMany(record("1164", "12", null, null), ctx("weekly_covid"));
    assertEquals(2, rows.size());
    Map<String, Object> allCause = byCause(rows, "All Cause");
    assertEquals("1164", allCause.get("deaths"));
    assertEquals("2020", allCause.get("year"));
    assertEquals("2020-02-29", allCause.get("week_ending_date"));
    assertEquals("Alabama", allCause.get("state"));
    assertEquals("weekly", allCause.get("source_type"));
    assertEquals("12", byCause(rows, "COVID-19").get("deaths"));
  }

  @Test void suppressedUnderlyingCountIsNullAndAllCauseSurvives() {
    String flag = "Suppressed (counts 1-9)";
    List<Map<String, Object>> rows =
        transformer.transformRecordToMany(record("1059", null, flag, flag), ctx("weekly_covid"));
    assertNull(byCause(rows, "COVID-19").get("deaths"));
    assertEquals("1059", byCause(rows, "All Cause").get("deaths"));
  }

  @Test void multipleCauseFlagDoesNotNullARealUnderlyingZero() {
    List<Map<String, Object>> rows = transformer.transformRecordToMany(
        record("1200", "0", "Suppressed (counts 1-9)", null), ctx("weekly_covid"));
    assertEquals("0", byCause(rows, "COVID-19").get("deaths"));
  }

  @Test void preCovidVintageStaysOneAllCauseRow() {
    Map<String, Object> r = new HashMap<>();
    r.put("jurisdiction_of_occurrence", "Alabama");
    r.put("mmwryear", "2019");
    r.put("weekendingdate", "2019-01-05");
    r.put("allcause", "1100");
    List<Map<String, Object>> rows = transformer.transformRecordToMany(r, ctx("weekly_precovid"));
    assertEquals(1, rows.size());
    assertEquals("All Cause", rows.get(0).get("cause_name"));
    assertEquals("1100", rows.get(0).get("deaths"));
    assertEquals("2019-01-05", rows.get(0).get("week_ending_date"));
  }

  @Test void annualStaysOneRow() {
    Map<String, Object> r = new HashMap<>();
    r.put("year", "2017");
    r.put("state", "Alabama");
    r.put("cause_name", "All causes");
    r.put("_113_cause_name", "All Causes");
    r.put("deaths", "50000");
    r.put("aadr", "900.1");
    List<Map<String, Object>> rows = transformer.transformRecordToMany(r, ctx("annual"));
    assertEquals(1, rows.size());
    assertEquals("annual", rows.get(0).get("source_type"));
    assertEquals("900.1", rows.get(0).get("age_adjusted_rate"));
  }

  @Test void singleRowContractRejectsCovidVintage() {
    assertThrows(UnsupportedOperationException.class,
        () -> transformer.transformRecord(record("1", "1", null, null), ctx("weekly_covid")));
  }

  @Test void stringPathFansOutAndUsesUnderlyingFlag() throws Exception {
    String json = "[{\"jurisdiction_of_occurrence\":\"Alabama\",\"mmwryear\":\"2020\","
        + "\"week_ending_date\":\"2020-03-21\",\"all_cause\":\"1059\","
        + "\"flag_cov19mcod\":\"Suppressed (counts 1-9)\","
        + "\"covid_19_u071_underlying_cause_of_death\":\"0\"}]";
    JsonNode out = MAPPER.readTree(transformer.transform(json, ctx("weekly_covid")));
    assertEquals(2, out.size());
    assertEquals("All Cause", out.get(0).path("cause_name").asText());
    assertEquals("1059", out.get(0).path("deaths").asText());
    assertEquals("COVID-19", out.get(1).path("cause_name").asText());
    assertEquals("0", out.get(1).path("deaths").asText());
  }
}
