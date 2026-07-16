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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code usaspending_by_state} — federal spending by
 * place-of-performance state for a fiscal year, from USAspending
 * {@code POST /api/v2/search/spending_by_geography/} ({@code geo_layer=state},
 * all award types). The federal fiscal year is expressed as an Oct-1 .. Sep-30
 * {@code time_period}.
 */
public class UsaSpendingStateProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsaSpendingStateProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ENDPOINT =
      "https://api.usaspending.gov/api/v2/search/spending_by_geography/";

  /** Contracts, IDVs, grants, direct payments, loans, and other. */
  private static final String AWARD_TYPE_CODES =
      "\"A\",\"B\",\"C\",\"D\",\"IDV_A\",\"IDV_B\",\"IDV_C\",\"IDV_D\",\"IDV_E\","
      + "\"02\",\"03\",\"04\",\"05\",\"06\",\"10\",\"07\",\"08\",\"09\",\"11\"";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("usaspending_by_state: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    int fy;
    try {
      fy = Integer.parseInt(year.trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("usaspending_by_state: non-numeric year {}", year);
      return Collections.emptyIterator();
    }
    String start = (fy - 1) + "-10-01";
    String end = fy + "-09-30";
    String body = "{\"filters\":{\"time_period\":[{\"start_date\":\"" + start + "\",\"end_date\":\"" + end
        + "\"}],\"award_type_codes\":[" + AWARD_TYPE_CODES + "]},"
        + "\"scope\":\"place_of_performance\",\"geo_layer\":\"state\","
        + "\"spending_level\":\"transactions\",\"subawards\":false}";
    LOGGER.info("usaspending_by_state: POST {} fy={}", ENDPOINT, fy);

    JsonNode root;
    InputStream in = FiscalHttp.openPostJson(ENDPOINT, body).getInputStream();
    try {
      root = MAPPER.readTree(in);
    } finally {
      in.close();
    }
    JsonNode results = root.path("results");
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    if (results.isArray()) {
      for (JsonNode r : results) {
        String code = text(r, "shape_code");
        if (code == null) {
          continue;
        }
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("state_abbr", code);
        row.put("state_name", text(r, "display_name"));
        row.put("obligated_amount", num(r, "aggregated_amount"));
        row.put("population", numLong(r, "population"));
        row.put("per_capita", num(r, "per_capita"));
        rows.add(row);
      }
    }
    LOGGER.info("usaspending_by_state: {} state rows for fy {}", rows.size(), fy);
    return rows.iterator();
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    String s = v.asText();
    return (s == null || s.trim().isEmpty()) ? null : s;
  }

  private static Double num(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    return FiscalHttp.toDouble(v.asText());
  }

  private static Long numLong(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    return FiscalHttp.toLong(v.asText());
  }
}
