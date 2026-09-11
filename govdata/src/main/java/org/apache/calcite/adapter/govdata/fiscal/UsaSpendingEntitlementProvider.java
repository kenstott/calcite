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

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.RawCache;
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
 * DataProvider for {@code entitlement_spending_by_state} — federal spending by
 * place-of-performance state for a fiscal year, filtered to specific entitlement
 * program CFDA/assistance-listing numbers, from USAspending
 * {@code POST /api/v2/search/spending_by_geography/} ({@code geo_layer=state},
 * {@code filters.program_numbers}). One request per program per fiscal year (the
 * endpoint accepts only a single geography/period slice per call, same as the
 * sibling {@link UsaSpendingStateProvider}), each cached independently so a
 * changed program list re-fetches only the affected program.
 *
 * <p>Medicare's two programs (93.773 / 93.774) inherit the same
 * place-of-performance distortion already documented on
 * {@code usaspending_by_state.obligated_amount_excl_loans_excl_cms_admin}: CMS
 * awards report place of performance at the Medicare Administrative
 * Contractor's location, not the beneficiary's, so states hosting a MAC (e.g.
 * ND/Noridian, MN, IN, KY) show implausibly large per-state totals. This is the
 * same known root cause, not a new anomaly — see that column's comment for the
 * FY2023 evidence. Social Security (96.002) is not affected; SSA benefit
 * payments report at the beneficiary's state.
 */
public class UsaSpendingEntitlementProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsaSpendingEntitlementProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String ENDPOINT =
      "https://api.usaspending.gov/api/v2/search/spending_by_geography/";

  /** program label (stored value) -> CFDA / assistance-listing number. */
  private static final Map<String, String> PROGRAMS = new LinkedHashMap<String, String>();

  static {
    PROGRAMS.put("social_security_retirement", "96.002");
    PROGRAMS.put("medicare_hospital_insurance", "93.773");
    PROGRAMS.put("medicare_supplementary_medical_insurance", "93.774");
  }

  private static String cacheKey(String cfdaNumber, String body) {
    return ENDPOINT + "/cfda-" + cfdaNumber + "?" + body;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("entitlement_spending_by_state: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    int fy;
    try {
      fy = Integer.parseInt(year.trim());
    // fallback-guard: allow narrow guard on a framework-supplied dimension value, before any download/parse; bad value is logged
    } catch (NumberFormatException e) {
      LOGGER.warn("entitlement_spending_by_state: non-numeric year {}", year);
      return Collections.emptyIterator();
    }
    String start = (fy - 1) + "-10-01";
    String end = fy + "-09-30";

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (Map.Entry<String, String> program : PROGRAMS.entrySet()) {
      String programLabel = program.getKey();
      String cfdaNumber = program.getValue();
      String body = "{\"filters\":{\"time_period\":[{\"start_date\":\"" + start + "\",\"end_date\":\""
          + end + "\"}],\"program_numbers\":[\"" + cfdaNumber + "\"]},"
          + "\"scope\":\"place_of_performance\",\"geo_layer\":\"state\","
          + "\"spending_level\":\"transactions\",\"subawards\":false}";
      LOGGER.info("entitlement_spending_by_state: POST {} fy={} program={} cfda={}",
          ENDPOINT, fy, programLabel, cfdaNumber);

      JsonNode root;
      InputStream in = rawCache.openStream(cacheKey(cfdaNumber, body),
          () -> FiscalHttp.openPostJsonWithRetry(ENDPOINT, body).getInputStream());
      try {
        root = MAPPER.readTree(in);
      } finally {
        in.close();
      }

      JsonNode results = root.path("results");
      if (results.isArray()) {
        for (JsonNode r : results) {
          String code = text(r, "shape_code");
          if (code == null) {
            continue;
          }
          Map<String, Object> row = new LinkedHashMap<String, Object>();
          row.put("state_abbr", code);
          row.put("state_name", text(r, "display_name"));
          row.put("program", programLabel);
          row.put("cfda_number", cfdaNumber);
          row.put("obligated_amount", num(r, "aggregated_amount"));
          rows.add(row);
        }
      }
    }
    LOGGER.info("entitlement_spending_by_state: {} rows for fy {}", rows.size(), fy);
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
}
