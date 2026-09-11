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
 * DataProvider for {@code usaspending_by_district} — federal spending by
 * place-of-performance congressional district for a fiscal year, from USAspending
 * {@code POST /api/v2/search/spending_by_geography/} ({@code geo_layer=district},
 * all award types). Same mechanism, cadence, and award-type/loan/CMS-admin column
 * shape as {@link UsaSpendingCountyProvider} and {@code UsaSpendingStateProvider} —
 * one call per fiscal year, 442 rows in a single unpaginated response (435 numbered
 * districts + at-large single-district states, DC, and territories).
 *
 * <p>{@code shape_code} (e.g. {@code "4204"}) is the 2-digit state FIPS + 2-digit
 * district number, matching {@code geo.congressional_districts.cd_fips} exactly —
 * confirmed live, no crosswalk needed. {@code display_name} (e.g. {@code "PA-04"})
 * decodes into state abbreviation + district number; district {@code "00"} means an
 * at-large single-district state and {@code "98"} means a non-voting delegate
 * district (DC, Puerto Rico, Guam, USVI, American Samoa, Northern Mariana Islands).
 *
 * <p>The endpoint also returns {@code population} and {@code per_capita}, but those
 * are USAspending's own reference figures and are 2020 decennial counts — stale by
 * several vintages and silently so. They are deliberately not stored: per-capita
 * belongs in a join against {@code census.acs_population} at query time, where the
 * population vintage is explicit.
 */
public class UsaSpendingDistrictProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsaSpendingDistrictProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static String cacheKey(String label, String body) {
    return ENDPOINT + "/" + label + "?" + body;
  }

  private static final String ENDPOINT =
      "https://api.usaspending.gov/api/v2/search/spending_by_geography/";

  /** Contracts, IDVs, grants, direct payments, loans, and other — matches by_state/by_county. */
  private static final String AWARD_TYPE_CODES =
      "\"A\",\"B\",\"C\",\"D\",\"IDV_A\",\"IDV_B\",\"IDV_C\",\"IDV_D\",\"IDV_E\","
      + "\"02\",\"03\",\"04\",\"05\",\"06\",\"10\",\"07\",\"08\",\"09\",\"11\"";

  /** Same set as {@link #AWARD_TYPE_CODES} minus the two loan award types ('07' direct
   * loans, '08' guaranteed/insured loans) — matches by_state/by_county's excl-loans figure. */
  private static final String AWARD_TYPE_CODES_EXCL_LOANS =
      "\"A\",\"B\",\"C\",\"D\",\"IDV_A\",\"IDV_B\",\"IDV_C\",\"IDV_D\",\"IDV_E\","
      + "\"02\",\"03\",\"04\",\"05\",\"06\",\"10\",\"09\",\"11\"";

  /** CMS-funded awards report place of performance at the Medicare Administrative
   * Contractor's location, not the beneficiary's -- matches by_state/by_county's
   * excl-CMS-admin figure. */
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
      LOGGER.warn("usaspending_by_district: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    int fy;
    try {
      fy = Integer.parseInt(year.trim());
    // fallback-guard: allow narrow guard on a framework-supplied dimension value, before any download/parse; bad value is logged
    } catch (NumberFormatException e) {
      LOGGER.warn("usaspending_by_district: non-numeric year {}", year);
      return Collections.emptyIterator();
    }
    String start = (fy - 1) + "-10-01";
    String end = fy + "-09-30";
    String body = "{\"filters\":{\"time_period\":[{\"start_date\":\"" + start + "\",\"end_date\":\"" + end
        + "\"}],\"award_type_codes\":[" + AWARD_TYPE_CODES + "]},"
        + "\"scope\":\"place_of_performance\",\"geo_layer\":\"district\","
        + "\"spending_level\":\"transactions\",\"subawards\":false}";
    LOGGER.info("usaspending_by_district: POST {} fy={}", ENDPOINT, fy);

    JsonNode root;
    InputStream in = rawCache.openStream(cacheKey("all", body),
        () -> FiscalHttp.openPostJsonWithRetry(ENDPOINT, body).getInputStream());
    try {
      root = MAPPER.readTree(in);
    } finally {
      in.close();
    }

    String bodyExclLoans =
        "{\"filters\":{\"time_period\":[{\"start_date\":\"" + start + "\",\"end_date\":\"" + end
        + "\"}],\"award_type_codes\":[" + AWARD_TYPE_CODES_EXCL_LOANS + "]},"
        + "\"scope\":\"place_of_performance\",\"geo_layer\":\"district\","
        + "\"spending_level\":\"transactions\",\"subawards\":false}";
    LOGGER.info("usaspending_by_district: POST {} fy={} (excl loans)", ENDPOINT, fy);
    JsonNode rootExclLoans;
    InputStream inExclLoans = rawCache.openStream(cacheKey("excl-loans", bodyExclLoans),
        () -> FiscalHttp.openPostJsonWithRetry(ENDPOINT, bodyExclLoans).getInputStream());
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
        + "]},\"scope\":\"place_of_performance\",\"geo_layer\":\"district\","
        + "\"spending_level\":\"transactions\",\"subawards\":false}";
    LOGGER.info("usaspending_by_district: POST {} fy={} (CMS only, excl loans)", ENDPOINT, fy);
    JsonNode rootCms;
    InputStream inCms = rawCache.openStream(cacheKey("cms-excl-loans", bodyCms),
        () -> FiscalHttp.openPostJsonWithRetry(ENDPOINT, bodyCms).getInputStream());
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
        String displayName = text(r, "display_name");
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("cd_fips", code);
        row.put("state_abbr", stateAbbrFromDisplayName(displayName));
        row.put("district_number", districtNumberFromDisplayName(displayName));
        row.put("cd_name", displayName);
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
    LOGGER.info("usaspending_by_district: {} district rows for fy {}", rows.size(), fy);
    return rows.iterator();
  }

  /** {@code "PA-04"} -&gt; {@code "PA"}; null-safe for a malformed/missing display_name. */
  private static String stateAbbrFromDisplayName(String displayName) {
    if (displayName == null) {
      return null;
    }
    int dash = displayName.indexOf('-');
    return dash > 0 ? displayName.substring(0, dash) : null;
  }

  /** {@code "PA-04"} -&gt; {@code "04"}; kept as text since "00" (at-large) and "98"
   * (non-voting delegate) are categorical markers, not a numeric ordinal. */
  private static String districtNumberFromDisplayName(String displayName) {
    if (displayName == null) {
      return null;
    }
    int dash = displayName.indexOf('-');
    return dash >= 0 && dash + 1 < displayName.length() ? displayName.substring(dash + 1) : null;
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
