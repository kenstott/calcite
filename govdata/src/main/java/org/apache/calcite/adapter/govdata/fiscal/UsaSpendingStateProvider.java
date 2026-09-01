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
 * DataProvider for {@code usaspending_by_state} — federal spending by
 * place-of-performance state for a fiscal year, from USAspending
 * {@code POST /api/v2/search/spending_by_geography/} ({@code geo_layer=state},
 * all award types). The federal fiscal year is expressed as an Oct-1 .. Sep-30
 * {@code time_period}.
 */
public class UsaSpendingStateProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsaSpendingStateProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  /**
   * Cache identity for one of this batch's POSTs. The three requests share an endpoint and differ
   * only in their body, so the label keeps the entries legible and the body rides in the query
   * position: the cache names the file from the path and digests the whole key, so a changed
   * request body re-keys itself instead of replaying the previous answer.
   */
  private static String cacheKey(String label, String body) {
    return ENDPOINT + "/" + label + "?" + body;
  }

  private static final String ENDPOINT =
      "https://api.usaspending.gov/api/v2/search/spending_by_geography/";

  /** Contracts, IDVs, grants, direct payments, loans, and other. */
  private static final String AWARD_TYPE_CODES =
      "\"A\",\"B\",\"C\",\"D\",\"IDV_A\",\"IDV_B\",\"IDV_C\",\"IDV_D\",\"IDV_E\","
      + "\"02\",\"03\",\"04\",\"05\",\"06\",\"10\",\"07\",\"08\",\"09\",\"11\"";

  /** Same set as {@link #AWARD_TYPE_CODES} minus the two loan award types ('07' direct
   * loans, '08' guaranteed/insured loans). Loans report face value at the lender/servicer's
   * place of performance rather than the borrower's, which inflates place-of-performance
   * totals for states with large loan servicers -- this excl-loans figure is a truer measure
   * of spending actually landing in a state. */
  private static final String AWARD_TYPE_CODES_EXCL_LOANS =
      "\"A\",\"B\",\"C\",\"D\",\"IDV_A\",\"IDV_B\",\"IDV_C\",\"IDV_D\",\"IDV_E\","
      + "\"02\",\"03\",\"04\",\"05\",\"06\",\"10\",\"09\",\"11\"";

  /** CMS-funded awards report place of performance at the Medicare Administrative
   * Contractor's location, not the beneficiary's -- e.g. Noridian Healthcare Solutions
   * (Fargo, ND) processes Medicare fee-for-service claims nationwide, but every claim
   * geocodes to ND. Confirmed nationwide, not an ND-specific anomaly: filtering to this
   * one funding sub-agency alone (FY2023) shows Minnesota at $165.8B and Indiana at
   * $133.4B -- both exceeding California's $125.5B despite population a fraction of the
   * size -- consistent with other states hosting large Medicare Administrative
   * Contractors. Subtracting it drops ND from $82.0B (implausible for its population) to
   * $6.98B; CA/TX/NY, where CMS is a much smaller share of the total (15-30%), stay large
   * and plausible. */
  private static final String CMS_AGENCY_FILTER =
      "\"agencies\":[{\"type\":\"funding\",\"tier\":\"subtier\","
      + "\"name\":\"Centers for Medicare and Medicaid Services\"}],";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
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
    // fallback-guard: allow narrow guard on a framework-supplied dimension value, before any download/parse; bad value is logged
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
    InputStream in = rawCache.openStream(cacheKey("all", body),
        () -> FiscalHttp.openPostJson(ENDPOINT, body).getInputStream());
    try {
      root = MAPPER.readTree(in);
    } finally {
      in.close();
    }

    String bodyExclLoans =
        "{\"filters\":{\"time_period\":[{\"start_date\":\"" + start + "\",\"end_date\":\"" + end
        + "\"}],\"award_type_codes\":[" + AWARD_TYPE_CODES_EXCL_LOANS + "]},"
        + "\"scope\":\"place_of_performance\",\"geo_layer\":\"state\","
        + "\"spending_level\":\"transactions\",\"subawards\":false}";
    LOGGER.info("usaspending_by_state: POST {} fy={} (excl loans)", ENDPOINT, fy);
    JsonNode rootExclLoans;
    InputStream inExclLoans = rawCache.openStream(cacheKey("excl-loans", bodyExclLoans),
        () -> FiscalHttp.openPostJson(ENDPOINT, bodyExclLoans).getInputStream());
    try {
      rootExclLoans = MAPPER.readTree(inExclLoans);
    } finally {
      inExclLoans.close();
    }
    Map<String, Double> exclLoansByCode = new LinkedHashMap<String, Double>();
    JsonNode resultsExclLoans = rootExclLoans.path("results");
    if (resultsExclLoans.isArray()) {
      for (JsonNode r : resultsExclLoans) {
        String code = text(r, "shape_code");
        if (code == null) {
          continue;
        }
        exclLoansByCode.put(code, num(r, "aggregated_amount"));
      }
    }

    String bodyCms =
        "{\"filters\":{\"time_period\":[{\"start_date\":\"" + start + "\",\"end_date\":\"" + end
        + "\"}]," + CMS_AGENCY_FILTER + "\"award_type_codes\":[" + AWARD_TYPE_CODES_EXCL_LOANS
        + "]},\"scope\":\"place_of_performance\",\"geo_layer\":\"state\","
        + "\"spending_level\":\"transactions\",\"subawards\":false}";
    LOGGER.info("usaspending_by_state: POST {} fy={} (CMS only, excl loans)", ENDPOINT, fy);
    JsonNode rootCms;
    InputStream inCms = rawCache.openStream(cacheKey("cms-excl-loans", bodyCms),
        () -> FiscalHttp.openPostJson(ENDPOINT, bodyCms).getInputStream());
    try {
      rootCms = MAPPER.readTree(inCms);
    } finally {
      inCms.close();
    }
    Map<String, Double> cmsByCode = new LinkedHashMap<String, Double>();
    JsonNode resultsCms = rootCms.path("results");
    if (resultsCms.isArray()) {
      for (JsonNode r : resultsCms) {
        String code = text(r, "shape_code");
        if (code == null) {
          continue;
        }
        cmsByCode.put(code, num(r, "aggregated_amount"));
      }
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
        row.put("obligated_amount_excl_loans", exclLoansByCode.get(code));
        Double exclLoans = exclLoansByCode.get(code);
        Double cms = cmsByCode.get(code);
        if (exclLoans != null) {
          row.put("obligated_amount_excl_loans_excl_cms_admin",
              exclLoans - (cms == null ? 0.0 : cms));
        }
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
}
