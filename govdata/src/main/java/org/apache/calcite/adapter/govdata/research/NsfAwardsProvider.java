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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * DataProvider for {@code nsf_award_projects} — award-level NSF grant microdata from the NSF Award
 * Search API ({@code GET /services/v1/awards.json}), one fetch per (fiscal year, award month)
 * dimension combo.
 *
 * <p>The API truncates every query at {@link #RESULT_CAP} results, reporting the truncated figure
 * as {@code totalCount}. A full year runs ~12,000 awards, so the fetch is sliced by the calendar
 * month of the award date; the busiest month on record (August 2018) is ~3,600. A slice that
 * reports the cap is rejected rather than accepted as a truncated prefix.
 *
 * <p>The fiscal year runs October through September, so fiscal year N's October-December slices
 * are calendar year N-1. Pages are read one at a time through the raw cache and streamed to the
 * caller; only the award ids of the current slice are retained, to drop rows that straddle a page
 * boundary (the API documents no sort tiebreaker) and to check the slice against its own
 * {@code totalCount} on exhaustion. The API's {@code offset} counts records from zero.
 */
public class NsfAwardsProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(NsfAwardsProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ENDPOINT = "https://api.nsf.gov/services/v1/awards.json";
  private static final int PAGE_SIZE = 100;
  // The API reports at most this many results for any query, however many match.
  private static final int RESULT_CAP = 10000;
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

  /** Walks one slice's pages lazily, holding a single page of the response at a time. */
  private static final class AwardIterator implements Iterator<Map<String, Object>> {
    private final int fiscalYear;
    private final String dateStart;
    private final String dateEnd;
    private final RawCache rawCache;
    private final Set<String> seenIds = new HashSet<String>();
    private Iterator<JsonNode> page = Collections.emptyIterator();
    private int nextOffset;
    private int totalCount = -1;
    private boolean exhausted;
    private Map<String, Object> pending;

    AwardIterator(int fiscalYear, YearMonth slice, RawCache rawCache) {
      this.fiscalYear = fiscalYear;
      this.dateStart = slice.atDay(1).format(API_DATE);
      this.dateEnd = slice.atEndOfMonth().format(API_DATE);
      this.rawCache = rawCache;
    }

    @Override public boolean hasNext() {
      while (pending == null) {
        if (page.hasNext()) {
          JsonNode award = page.next();
          String id = text(award, "id");
          if (id == null) {
            throw new IllegalStateException("nsf_award_projects: award without an id in "
                + dateStart + ".." + dateEnd);
          }
          if (seenIds.add(id)) {
            pending = toRow(award);
          }
        } else if (exhausted) {
          checkComplete();
          return false;
        } else {
          loadNextPage();
        }
      }
      return true;
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = pending;
      pending = null;
      return row;
    }

    private void loadNextPage() {
      String url = ENDPOINT + "?dateStart=" + dateStart + "&dateEnd=" + dateEnd
          + "&rpp=" + PAGE_SIZE + "&offset=" + nextOffset;
      JsonNode response;
      try (InputStream in = rawCache.openStream(url, () -> rawGet(url))) {
        response = MAPPER.readTree(in).path("response");
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      if (totalCount < 0) {
        totalCount = response.path("metadata").path("totalCount").asInt(-1);
        if (totalCount < 0) {
          throw new IllegalStateException("nsf_award_projects: no totalCount in response for "
              + url + ": " + response);
        }
        if (totalCount >= RESULT_CAP) {
          throw new IllegalStateException("nsf_award_projects: slice " + dateStart + ".."
              + dateEnd + " reports " + totalCount + " awards, the API's result cap; the slice "
              + "is truncated and needs a finer split");
        }
      }
      JsonNode awards = response.path("award");
      int size = awards.isArray() ? awards.size() : 0;
      page = size == 0 ? Collections.<JsonNode>emptyIterator() : awards.iterator();
      if (size < PAGE_SIZE) {
        exhausted = true;
      }
      nextOffset += PAGE_SIZE;
    }

    private void checkComplete() {
      if (seenIds.size() != totalCount) {
        throw new IllegalStateException("nsf_award_projects: slice " + dateStart + ".." + dateEnd
            + " returned " + seenIds.size() + " distinct awards but the API reports " + totalCount);
      }
      LOGGER.info("nsf_award_projects: {} awards for fy={} {}..{}", seenIds.size(), fiscalYear,
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
