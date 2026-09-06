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
import org.apache.calcite.adapter.file.etl.CrossProcessRateLimiter;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

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
 * DataProvider for {@code usaspending_recipients_by_district} — the top 100
 * recipients by obligated dollar amount for each place-of-performance congressional
 * district, per fiscal year.
 *
 * <p>Two USAspending endpoints, both {@code POST}: first, one call per fiscal year
 * to {@code /api/v2/search/spending_by_geography/} (geo_layer=district, same as
 * {@link UsaSpendingDistrictProvider}) to enumerate the 442 current districts for
 * that year — this avoids hard-coding a district list that redistricting would make
 * stale. Then one call per (fiscal year, district) to
 * {@code /api/v2/search/spending_by_category/recipient/} with a
 * {@code place_of_performance_locations} filter shaped
 * {@code {"country":"USA","state":"PA","district_current":"04"}}, which returns a
 * server-side pre-aggregated, pre-ranked (descending by amount) list of recipients
 * for that district/year in a single unpaginated call — confirmed live: {@code limit}
 * caps at 100 per page (500 is rejected as "above max '100'"), which is exactly the
 * top-N cut this table stores, so no pagination is needed. This is the same order of
 * magnitude of API traffic as {@link UsaSpendingDistrictProvider} (3 calls/year x 442
 * rows already) plus one call per district — self-throttled via
 * {@link CrossProcessRateLimiter} at 1 request/second, matching the rate this host's
 * sibling fiscal tables declare in their {@code source.rateLimit} config.
 *
 * <p>Confirmed live: at this call volume, api.usaspending.gov intermittently returns a
 * transient HTTP 500 or drops the connection mid-response ("Unexpected end of file from
 * server") on an otherwise-valid request. Both POST calls go through
 * {@link FiscalHttp#openPostJsonWithRetry}, which retries those (and 429/502/503/504)
 * with exponential backoff rather than failing the whole fiscal-year batch on one bad
 * connection.
 *
 * <p><b>Scope: top 100 recipients per district per year</b>, matching the API's own
 * page-size ceiling — not an attempt at every recipient in a district. A district
 * with concentrated spending (a hospital system, a university, a large contractor)
 * is fully captured; a district with many small, dispersed recipients is not
 * exhaustively enumerated below the top 100. An aggregate {@code "MULTIPLE
 * RECIPIENTS"} entry (with null id fields) appears in many districts for
 * sub-reporting-threshold transactions bucketed together by USAspending itself, and
 * is kept as a real ranked row rather than filtered out.
 *
 * <p>{@code cd_fips} matches {@code geo.congressional_districts.cd_fips} exactly,
 * decoded the same way as {@link UsaSpendingDistrictProvider}.
 */
public class UsaSpendingDistrictRecipientsProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UsaSpendingDistrictRecipientsProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String GEOGRAPHY_ENDPOINT =
      "https://api.usaspending.gov/api/v2/search/spending_by_geography/";
  private static final String RECIPIENT_ENDPOINT =
      "https://api.usaspending.gov/api/v2/search/spending_by_category/recipient/";

  /** Top-N cut, matching the API's own per-page maximum (500 is rejected). */
  private static final int TOP_N = 100;

  /** Host-wide pace for the per-district loop; the one-off district-list call
   * riding along at the head of each year is not separately throttled. */
  private static final String RATE_LIMIT_KEY = "api.usaspending.gov:spending-by-category";
  private static final long RATE_LIMIT_INTERVAL_MS = 1000L;

  /** Contracts, IDVs, grants, direct payments, loans, and other — matches
   * {@link UsaSpendingDistrictProvider}'s all-award-types figure. */
  private static final String AWARD_TYPE_CODES =
      "\"A\",\"B\",\"C\",\"D\",\"IDV_A\",\"IDV_B\",\"IDV_C\",\"IDV_D\",\"IDV_E\","
      + "\"02\",\"03\",\"04\",\"05\",\"06\",\"10\",\"07\",\"08\",\"09\",\"11\"";

  private static String cacheKey(String endpoint, String label, String body) {
    return endpoint + "/" + label + "?" + body;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("usaspending_recipients_by_district: no year in dimension variables {}",
          variables);
      return Collections.emptyIterator();
    }
    int fy;
    try {
      fy = Integer.parseInt(year.trim());
    // fallback-guard: allow narrow guard on a framework-supplied dimension value, before any download/parse; bad value is logged
    } catch (NumberFormatException e) {
      LOGGER.warn("usaspending_recipients_by_district: non-numeric year {}", year);
      return Collections.emptyIterator();
    }
    String start = (fy - 1) + "-10-01";
    String end = fy + "-09-30";

    List<String[]> districts = fetchDistrictList(rawCache, start, end, fy);
    LOGGER.info("usaspending_recipients_by_district: {} districts for fy {}",
        districts.size(), fy);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (String[] d : districts) {
      String cdFips = d[0];
      String stateAbbr = d[1];
      String districtNumber = d[2];
      if (stateAbbr == null || districtNumber == null) {
        continue;
      }
      String body = "{\"filters\":{\"time_period\":[{\"start_date\":\"" + start
          + "\",\"end_date\":\"" + end + "\"}],"
          + "\"place_of_performance_locations\":[{\"country\":\"USA\",\"state\":\""
          + stateAbbr + "\",\"district_current\":\"" + districtNumber + "\"}],"
          + "\"award_type_codes\":[" + AWARD_TYPE_CODES + "]},"
          + "\"category\":\"recipient\",\"spending_level\":\"transactions\","
          + "\"limit\":" + TOP_N + ",\"page\":1}";

      JsonNode root;
      InputStream in = rawCache.openStream(
          cacheKey(RECIPIENT_ENDPOINT, "fy" + fy + "-" + cdFips, body), () -> {
            CrossProcessRateLimiter.acquire(RATE_LIMIT_KEY, RATE_LIMIT_INTERVAL_MS);
            return FiscalHttp.openPostJsonWithRetry(RECIPIENT_ENDPOINT, body).getInputStream();
          });
      try {
        root = MAPPER.readTree(in);
      } finally {
        in.close();
      }

      JsonNode results = root.path("results");
      if (!results.isArray()) {
        continue;
      }
      int rank = 0;
      for (JsonNode r : results) {
        rank++;
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("cd_fips", cdFips);
        row.put("state_abbr", stateAbbr);
        row.put("district_number", districtNumber);
        row.put("rank", Integer.valueOf(rank));
        row.put("recipient_name", text(r, "name"));
        row.put("recipient_id", text(r, "recipient_id"));
        row.put("recipient_uei", text(r, "uei"));
        row.put("recipient_duns", text(r, "code"));
        row.put("obligated_amount", num(r, "amount"));
        rows.add(row);
      }
    }
    LOGGER.info("usaspending_recipients_by_district: {} recipient rows for fy {}",
        rows.size(), fy);
    return rows.iterator();
  }

  /**
   * Enumerates the fiscal year's current congressional districts via one call to
   * {@code spending_by_geography} (geo_layer=district) — the same source
   * {@link UsaSpendingDistrictProvider} uses for its own rows, ridden here purely to
   * get the (cd_fips, state_abbr, district_number) list without hard-coding it.
   * Cached under a distinct label from the sibling provider's own cache entries so
   * the two providers' raw-cache reads don't collide on the same body/key.
   */
  private List<String[]> fetchDistrictList(RawCache rawCache, String start, String end, int fy)
      throws IOException {
    String body = "{\"filters\":{\"time_period\":[{\"start_date\":\"" + start
        + "\",\"end_date\":\"" + end + "\"}],\"award_type_codes\":[" + AWARD_TYPE_CODES + "]},"
        + "\"scope\":\"place_of_performance\",\"geo_layer\":\"district\","
        + "\"spending_level\":\"transactions\",\"subawards\":false}";
    LOGGER.info("usaspending_recipients_by_district: POST {} fy={} (district enumeration)",
        GEOGRAPHY_ENDPOINT, fy);
    JsonNode root;
    InputStream in = rawCache.openStream(
        cacheKey(GEOGRAPHY_ENDPOINT, "district-list", body),
        () -> FiscalHttp.openPostJsonWithRetry(GEOGRAPHY_ENDPOINT, body).getInputStream());
    try {
      root = MAPPER.readTree(in);
    } finally {
      in.close();
    }
    List<String[]> districts = new ArrayList<String[]>();
    JsonNode results = root.path("results");
    if (results.isArray()) {
      for (JsonNode r : results) {
        String code = text(r, "shape_code");
        if (code == null) {
          continue;
        }
        String displayName = text(r, "display_name");
        districts.add(new String[] {
            code, stateAbbrFromDisplayName(displayName), districtNumberFromDisplayName(displayName)
        });
      }
    }
    return districts;
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
