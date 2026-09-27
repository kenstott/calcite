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
package org.apache.calcite.adapter.govdata.research;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code nsf_award_projects} — award-level NSF grant microdata from the NSF Award
 * Search API ({@code GET /services/v1/awards.json}), one fetch per (fiscal year, award month)
 * dimension combo.
 *
 * <p>Two API behaviors, both confirmed live, rule out ordinary offset pagination:
 * <ul>
 *   <li>The API has no documented sort order, and two identical requests for the same date range
 *       return the same {@code totalCount} but a different relative order of results. A record
 *       that shifts position between two page fetches for the same range can fall on both sides of
 *       an offset boundary and never appear on any page — silent data loss, not a duplicate.
 *   <li>A single response's {@code award} array is silently truncated at {@link #MAX_PAGE_RESULTS}
 *       regardless of the requested {@code rpp} — confirmed live, {@code rpp=5000} and
 *       {@code rpp=10000} both returned exactly 3,000 awards for a 3,025-award date range, with no
 *       error and a {@code totalCount} that (unlike the array) is accurate and uncapped.
 * </ul>
 * Since re-requesting the same range is unsound (first bullet) and a single request cannot be
 * trusted above the cap (second bullet), every date range this provider queries is queried exactly
 * once: {@link AwardIterator} fetches the whole month in one request, and only when that request's
 * {@code award} array comes back shorter than its {@code totalCount} (truncated) does it fall back
 * to bisecting the date range and recursing on each half — never re-fetching the range that was
 * truncated. Different halves are disjoint date windows, so this never repeats a query. A single
 * calendar day still reporting a truncated {@code totalCount} (unsplittable) is rejected rather than
 * accepted as a partial day; not seen in practice (max daily volume checked live: 252).
 *
 * <p>The fiscal year runs October through September, so fiscal year N's October-December slices
 * are calendar year N-1.
 */
public class NsfAwardsProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(NsfAwardsProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ENDPOINT = "https://api.nsf.gov/services/v1/awards.json";
  // Confirmed live: the API truncates any single response's award array to this many records
  // regardless of the rpp requested. Requesting exactly this many per request costs nothing extra
  // (the API never returns more anyway) and makes the truncation check a plain size comparison.
  private static final int MAX_PAGE_RESULTS = 3000;
  private static final DateTimeFormatter API_DATE = DateTimeFormatter.ofPattern("MM/dd/yyyy");
  private static final int FISCAL_YEAR_START_MONTH = 10;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    int fiscalYear = Integer.parseInt(required(variables, "year"));
    int month = Integer.parseInt(required(variables, "award_month"));
    int calendarYear = month >= FISCAL_YEAR_START_MONTH ? fiscalYear - 1 : fiscalYear;
    YearMonth slice = YearMonth.of(calendarYear, month);
    return new AwardIterator(fiscalYear, slice, rawCache);
  }

  private static String required(Map<String, String> variables, String name) {
    String value = variables.get(name);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalStateException("nsf_award_projects: dimension variable '" + name
          + "' is missing from " + variables);
    }
    return value.trim();
  }

  /** Fetches one slice's entire result set, recursively splitting on truncation (class javadoc). */
  private static final class AwardIterator implements Iterator<Map<String, Object>> {
    private final int fiscalYear;
    private final LocalDate monthStart;
    private final LocalDate monthEnd;
    private final RawCache rawCache;
    private Iterator<Map<String, Object>> rows;

    AwardIterator(int fiscalYear, YearMonth slice, RawCache rawCache) {
      this.fiscalYear = fiscalYear;
      this.monthStart = slice.atDay(1);
      this.monthEnd = slice.atEndOfMonth();
      this.rawCache = rawCache;
    }

    @Override public boolean hasNext() {
      ensureLoaded();
      return rows.hasNext();
    }

    @Override public Map<String, Object> next() {
      ensureLoaded();
      return rows.next();
    }

    private void ensureLoaded() {
      if (rows == null) {
        List<Map<String, Object>> collected = new ArrayList<Map<String, Object>>();
        fetchRange(monthStart, monthEnd, collected);
        rows = collected.iterator();
      }
    }

    /** Fetches [start, end] in one request, recursing on disjoint halves only if truncated. */
    private void fetchRange(LocalDate start, LocalDate end, List<Map<String, Object>> out) {
      String dateStart = start.format(API_DATE);
      String dateEnd = end.format(API_DATE);
      String url = ENDPOINT + "?dateStart=" + dateStart + "&dateEnd=" + dateEnd
          + "&rpp=" + MAX_PAGE_RESULTS + "&offset=0";
      JsonNode response;
      try (InputStream in = rawCache.openStream(url, () -> rawGet(url))) {
        response = MAPPER.readTree(in).path("response");
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      int totalCount = response.path("metadata").path("totalCount").asInt(-1);
      if (totalCount < 0) {
        throw new IllegalStateException("nsf_award_projects: no totalCount in response for "
            + url + ": " + response);
      }
      JsonNode awards = response.path("award");
      int size = awards.isArray() ? awards.size() : 0;
      if (size < totalCount) {
        if (start.equals(end)) {
          throw new IllegalStateException("nsf_award_projects: " + dateStart + " alone reports "
              + totalCount + " awards, over the API's " + MAX_PAGE_RESULTS + "-result response "
              + "cap; cannot split a single day further");
        }
        long days = ChronoUnit.DAYS.between(start, end);
        LocalDate mid = start.plusDays(days / 2);
        fetchRange(start, mid, out);
        fetchRange(mid.plusDays(1), end, out);
        return;
      }
      for (JsonNode award : awards) {
        String id = text(award, "id");
        if (id == null) {
          throw new IllegalStateException("nsf_award_projects: award without an id in "
              + dateStart + ".." + dateEnd);
        }
        out.add(toRow(award));
      }
      LOGGER.info("nsf_award_projects: {} awards for fy={} {}..{}", totalCount, fiscalYear,
          dateStart, dateEnd);
    }

    private Map<String, Object> toRow(JsonNode a) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("award_id", text(a, "id"));
      row.put("fiscal_year", fiscalYear);
      row.put("award_date", isoDate(a, "date"));
      row.put("start_date", isoDate(a, "startDate"));
      row.put("exp_date", isoDate(a, "expDate"));
      row.put("title", text(a, "title"));
      row.put("transaction_type", text(a, "transType"));
      row.put("fund_program_name", text(a, "fundProgramName"));
      row.put("cfda_number", text(a, "cfdaNumber"));
      row.put("directorate", text(a, "dirAbbr"));
      row.put("division", text(a, "divAbbr"));
      row.put("funds_obligated_amt", number(a, "fundsObligatedAmt"));
      row.put("estimated_total_amt", number(a, "estimatedTotalAmt"));
      row.put("awardee_name", text(a, "awardeeName"));
      row.put("awardee_uei", text(a, "ueiNumber"));
      row.put("awardee_city", text(a, "awardeeCity"));
      row.put("awardee_state", text(a, "awardeeStateCode"));
      row.put("awardee_country", text(a, "awardeeCountryCode"));
      row.put("awardee_district", text(a, "awardeeDistrictCode"));
      return row;
    }
  }

  /**
   * Issues the GET, failing on a non-2xx rather than returning the error body as content: the raw
   * cache commits an entry only when the supplier returns without error, and a cached error body
   * would be indistinguishable from a real page forever after.
   */
  private static InputStream rawGet(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    int status = conn.getResponseCode();
    if (status < 200 || status >= 300) {
      StringBuilder err = new StringBuilder();
      InputStream es = conn.getErrorStream();
      if (es != null) {
        try (BufferedReader r = new BufferedReader(
            new InputStreamReader(es, StandardCharsets.UTF_8))) {
          String line;
          while ((line = r.readLine()) != null) {
            err.append(line);
          }
        }
      }
      throw new IOException("NSF Award Search HTTP " + status + " for " + url + ": " + err);
    }
    return conn.getInputStream();
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    String s = v.asText().trim();
    return s.isEmpty() ? null : s;
  }

  /** The API renders dates as MM/dd/yyyy; the column carries ISO-8601. */
  private static String isoDate(JsonNode node, String field) {
    String s = text(node, field);
    return s == null ? null : LocalDate.parse(s, API_DATE).toString();
  }

  /** The API renders amounts as strings of whole dollars. */
  private static Double number(JsonNode node, String field) {
    String s = text(node, field);
    return s == null ? null : Double.valueOf(s);
  }
}
