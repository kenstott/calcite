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
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The forecast builder against canned venue responses and canned series rows: no network, no
 * catalog. Each series is small enough that the expected distribution is worked out by hand in
 * the test that uses it.
 */
@Tag("unit")
class MarketForecastsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-10-02T00:00:00Z");
  private static final String ID = "KXTEST-26SEP";
  private static final double EPS = 1e-9;

  /** Answers a URL from the first registered prefix it starts with. */
  private static final class FakeFetcher implements PredictionMarkets.Fetcher {
    final Map<String, JsonNode> byPrefix = new LinkedHashMap<>();

    @Override public JsonNode get(String url) throws IOException {
      for (Map.Entry<String, JsonNode> e : byPrefix.entrySet()) {
        if (url.startsWith(e.getKey())) {
          return e.getValue();
        }
      }
      throw new IOException("no canned response for " + url);
    }
  }

  /** Answers a query with the rows registered for the first key it contains, and keeps it. */
  private static final class FakeSql implements MarketTools.SqlRunner {
    final Map<String, ArrayNode> byKey = new LinkedHashMap<>();
    final List<String> queries = new ArrayList<>();

    FakeSql on(String key, ArrayNode rows) {
      byKey.put(key, rows);
      return this;
    }

    @Override public ArrayNode rows(String sql, int limit) {
      queries.add(sql);
      for (Map.Entry<String, ArrayNode> e : byKey.entrySet()) {
        if (sql.contains(e.getKey())) {
          return e.getValue();
        }
      }
      throw new AssertionError("no rows registered for: " + sql);
    }
  }

  // ─── Fixtures ──────────────────────────────────────────────────────────────

  private static ObjectNode market(String ticker, double strike, String title, String rules,
      String closeTime) {
    ObjectNode m = MAPPER.createObjectNode();
    m.put("ticker", ticker);
    m.put("status", "active");
    m.put("title", title);
    m.put("yes_bid_dollars", "0.4000");
    m.put("yes_ask_dollars", "0.4400");
    m.put("last_price_dollars", "0.4000");
    m.put("volume_fp", "5000.00");
    m.put("volume_24h_fp", "800.00");
    m.put("open_interest_fp", "2000.00");
    m.put("close_time", closeTime);
    m.put("strike_type", "greater");
    m.put("floor_strike", strike);
    m.put("rules_primary", rules);
    return m;
  }

  private static FakeFetcher venue(String title, String rules, String closeTime,
      double strike) {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("title", title);
    ev.put("event_ticker", ID);
    ev.put("series_ticker", "KXTEST");
    ev.put("category", "Economics");
    ev.putArray("settlement_sources").addObject().put("name", "BLS");
    ev.putArray("markets").add(market(ID + "-T" + strike, strike, title, rules, closeTime));
    FakeFetcher f = new FakeFetcher();
    ObjectNode series = MAPPER.createObjectNode();
    series.putObject("series").put("fee_type", "quadratic").put("fee_multiplier", 1);
    f.byPrefix.put(PredictionMarkets.KALSHI + "/series/KXTEST", series);
    ObjectNode one = MAPPER.createObjectNode();
    one.set("event", ev);
    f.byPrefix.put(PredictionMarkets.KALSHI + "/events/" + ID, one);
    return f;
  }

  /** A BLS-style table: one row per month, year and period columns. */
  private static ArrayNode blsRows(YearMonth start, double... values) {
    ArrayNode rows = MAPPER.createArrayNode();
    for (int i = 0; i < values.length; i++) {
      YearMonth ym = start.plusMonths(i);
      ObjectNode r = rows.addObject();
      r.put("year", ym.getYear());
      r.put("period", String.format("M%02d", ym.getMonthValue()));
      r.put("value", values[i]);
    }
    return rows;
  }

  /** A date-and-value table, one row per month, dated the first. */
  private static ArrayNode monthlyRows(YearMonth start, double... values) {
    ArrayNode rows = MAPPER.createArrayNode();
    for (int i = 0; i < values.length; i++) {
      ObjectNode r = rows.addObject();
      r.put("date", start.plusMonths(i).atDay(1).toString());
      r.put("value", values[i]);
    }
    return rows;
  }

  private static ArrayNode dailyRows(LocalDate start, double... values) {
    ArrayNode rows = MAPPER.createArrayNode();
    for (int i = 0; i < values.length; i++) {
      ObjectNode r = rows.addObject();
      r.put("date", start.plusDays(i).toString());
      r.put("value", values[i]);
    }
    return rows;
  }

  /** Unemployment from 2025-01 to 2026-08: 4.0, 4.1, 4.2 repeating, so the last is 4.1. */
  private static double[] unemployment() {
    double[] v = new double[20];
    for (int t = 0; t < v.length; t++) {
      v[t] = 4.0 + 0.1 * (t % 3);
    }
    return v;
  }

  private static final String UNEMPLOYMENT_RULES = "Resolves on the BLS unemployment rate "
      + "(U-3), seasonally adjusted, rounded to one decimal, in percent.";
  private static final String CLOSE = "2026-10-09T12:30:00Z";

  private static JsonNode args(String json) throws Exception {
    return MAPPER.readTree(json.replace('\'', '"'));
  }

  private static JsonNode run(FakeFetcher f, FakeSql sql, String extra) throws Exception {
    String spec = "{'source':'kalshi','event_id':'" + ID + "'" + extra + "}";
    return MAPPER.readTree(new MarketForecasts(f, sql, () -> NOW)
        .forecastMarketEvent(args(spec)));
  }

  private static JsonNode unemploymentRun(String extra) throws Exception {
    FakeSql sql = new FakeSql().on("LNS14000000",
        blsRows(YearMonth.of(2025, 1), unemployment()));
    return run(venue("Unemployment rate in September 2026", UNEMPLOYMENT_RULES, CLOSE, 4.0),
        sql, extra);
  }

  private static List<String> flagCodes(JsonNode out) {
    List<String> codes = new ArrayList<>();
    for (JsonNode f : out.get("flags")) {
      assertFalse(f.get("message").asText().isEmpty());
      codes.add(f.get("code").asText());
    }
    return codes;
  }

  // ─── Transforms ────────────────────────────────────────────────────────────

  /**
   * One-period changes are +0.1, +0.1, -0.2 repeating; of the 19 starts, 13 are rises and 6 are
   * falls, so the samples are 4.1 + 0.1 (13 times) and 4.1 - 0.2 (6 times).
   */
  @Test void levelAppliesHistoricalChangesToTheLatestLevel() throws Exception {
    JsonNode out = unemploymentRun("");
    assertEquals("forecast", out.get("status").asText());
    assertEquals("LNS14000000", out.get("series").asText());
    assertEquals("econ.employment_statistics", out.get("table").asText());
    assertEquals("level", out.get("transform").asText());
    assertEquals("series_default", out.get("transform_source").asText());
    assertEquals("2026-08", out.get("last_period").asText());
    assertEquals("2026-09", out.get("settlement_period").asText());
    assertEquals(1, out.get("steps_ahead").asInt());
    assertEquals(19, out.get("n").asInt());
    assertEquals(4.2, out.get("median").asDouble(), EPS);
    assertEquals(3.9, out.get("p05").asDouble(), EPS);
    assertEquals(4.2, out.get("p95").asDouble(), EPS);
    assertEquals(1, out.get("round").asInt());
    assertEquals(19, out.get("samples").size());
    assertTrue(out.get("method").asText().contains("additive"));
    assertEquals(0, out.get("flags").size());
  }

  /** Payrolls: the one-period level changes alternate 150 and 50 (10 and 9 of the 19). */
  @Test void changeSamplesTheOnePeriodDifferenceOfTheLevel() throws Exception {
    double[] v = new double[20];
    for (int t = 0; t < v.length; t++) {
      v[t] = 159000 + 100.0 * t + (t % 2 == 0 ? 0 : 50);
    }
    FakeSql sql = new FakeSql().on("CES0000000001", blsRows(YearMonth.of(2025, 1), v));
    JsonNode out = run(venue("Jobs added in September 2026",
        "Total nonfarm payrolls change, in thousands, seasonally adjusted, no decimals.",
        CLOSE, 100), sql, "");
    assertEquals("CES0000000001", out.get("series").asText());
    assertEquals("change", out.get("transform").asText());
    assertEquals(19, out.get("n").asInt());
    assertEquals(150, out.get("median").asDouble(), EPS);
    assertEquals(50, out.get("p05").asDouble(), EPS);
    assertEquals(150, out.get("p95").asDouble(), EPS);
    assertEquals(0, out.get("round").asInt());
    assertEquals("thousands", out.get("units").asText());
    assertEquals(0, out.get("flags").size());
  }

  /** Seasonally adjusted CPI rising 1% then 2% alternately: 12 of each in 24 changes. */
  @Test void monthOverMonthSamplesHistoricalPercentChanges() throws Exception {
    double[] v = new double[25];
    v[0] = 100;
    for (int i = 1; i < v.length; i++) {
      v[i] = v[i - 1] * (i % 2 == 1 ? 1.01 : 1.02);
    }
    FakeSql sql = new FakeSql().on("CPIAUCSL", monthlyRows(YearMonth.of(2024, 8), v));
    JsonNode out = run(venue("CPI inflation in September 2026",
        "BLS CPI-U month-over-month change, seasonally adjusted, one decimal, in percent.",
        CLOSE, 0.3), sql, "");
    assertEquals("CPIAUCSL", out.get("series").asText());
    assertEquals("econ.fred_indicators", out.get("table").asText());
    assertEquals("mom_pct", out.get("transform").asText());
    assertEquals("rules", out.get("transform_source").asText());
    assertEquals(24, out.get("n").asInt());
    assertEquals(1.5, out.get("median").asDouble(), EPS);
    assertEquals(1.0, out.get("p05").asDouble(), EPS);
    assertEquals(2.0, out.get("p95").asDouble(), EPS);
    assertFalse(out.get("method").asText().contains("calendar"));
    assertEquals(0, out.get("flags").size());
    assertTrue(sql.queries.get(0).contains("\"series\" = 'CPIAUCSL'"));
  }

  /**
   * A series not known to be seasonally adjusted is sampled only at the settlement calendar
   * month: 2016-09 to 2025-09 is ten Septembers.
   */
  @Test void monthOverMonthOnAnUnadjustedSeriesKeepsTheCalendarMonth() throws Exception {
    double[] v = new double[128];
    for (int t = 0; t < v.length; t++) {
      v[t] = 100 * Math.pow(1.002, t);
    }
    FakeSql sql = new FakeSql().on("CUUR0000SA0", blsRows(YearMonth.of(2016, 1), v));
    JsonNode out = run(venue("CPI inflation in September 2026",
        "BLS CPI-U month-over-month change, one decimal, in percent.", CLOSE, 0.3), sql,
        ",'series':'CUUR0000SA0'");
    assertEquals("CUUR0000SA0", out.get("series").asText());
    assertEquals("econ.inflation_metrics", out.get("table").asText());
    assertEquals(10, out.get("n").asInt());
    assertEquals(0.2, out.get("median").asDouble(), EPS);
    assertTrue(out.get("method").asText().contains("calendar month"));
    assertEquals(0, out.get("flags").size());
  }

  /** Kalshi titles some events "Apr 2026" and names the month in full only in the rules. */
  @Test void theSettlementMonthIsReadFromAnAbbreviatedTitleAndFromTheRulesYear()
      throws Exception {
    double[] v = new double[32];
    for (int t = 0; t < v.length; t++) {
      v[t] = 100 * Math.pow(1.0025, t);
    }
    String rules = "BLS CPI-U 12-month change, not seasonally adjusted, one decimal, in percent.";
    FakeSql sql = new FakeSql().on("CUUR0000SA0", blsRows(YearMonth.of(2024, 1), v));
    JsonNode out = run(venue("Inflation in Sep 2026 (CPI YoY)", rules, CLOSE, 3.0), sql, "");
    assertEquals("2026-09", out.get("settlement_period").asText(), out.toString());

    // No month in the title: the rules' month and its year, not the close time's.
    out = run(venue("CPI inflation (YoY)", "If CPI increases by more than 3.0% in the twelve "
        + "months ending September 2026, the market resolves to Yes. " + rules, CLOSE, 3.0),
        sql, "");
    assertEquals("2026-09", out.get("settlement_period").asText(), out.toString());
  }

  /** Steady 0.25% monthly growth gives a constant 12-month change, so every sample is it. */
  @Test void yearOverYearAppliesChangesInTheTwelveMonthRateToTheLatestRate() throws Exception {
    double[] v = new double[32];
    for (int t = 0; t < v.length; t++) {
      v[t] = 100 * Math.pow(1.0025, t);
    }
    FakeSql sql = new FakeSql().on("CUUR0000SA0", blsRows(YearMonth.of(2024, 1), v));
    JsonNode out = run(venue("CPI inflation in September 2026",
        "BLS CPI-U 12-month change, not seasonally adjusted, one decimal, in percent.",
        CLOSE, 3.0), sql, "");
    assertEquals("CUUR0000SA0", out.get("series").asText());
    assertEquals("yoy_pct", out.get("transform").asText());
    assertEquals(19, out.get("n").asInt());
    assertEquals(3.0, out.get("median").asDouble(), EPS);
    assertEquals(3.0, out.get("p05").asDouble(), EPS);
    assertEquals(3.0, out.get("p95").asDouble(), EPS);
    assertEquals(100 * (Math.pow(1.0025, 12) - 1), out.get("last_value").asDouble(), 1e-6);
    assertEquals(0, out.get("flags").size());
  }

  /** WTI alternates 100 and 101 daily, last 101; 14 days to close is 10 trading steps. */
  private static FakeSql oil(double... overrides) {
    double[] v = new double[30];
    for (int i = 0; i < v.length; i++) {
      v[i] = 100 + (i % 2);
    }
    for (int i = 0; i + 1 < overrides.length; i += 2) {
      v[(int) overrides[i]] = overrides[i + 1];
    }
    return new FakeSql().on("DCOILWTICO", dailyRows(LocalDate.of(2026, 9, 1), v));
  }

  private static FakeFetcher oilVenue() {
    return venue("Will WTI crude touch $90 in October 2026?",
        "Resolves Yes if the WTI spot price in dollars per barrel touches $90 at any time "
        + "before the close.", "2026-10-14T20:00:00Z", 90);
  }

  /**
   * From a start at 100 the path alternates 101 and 100 scaled by 1.01, peaking at 102.01; from
   * a start at 101 it never passes the latest level 101. Of 20 starts, 10 are of each.
   */
  @Test void maxPathTakesTheHighestPointOfEachHistoricalPath() throws Exception {
    JsonNode out = run(oilVenue(), oil(), "");
    assertEquals("DCOILWTICO", out.get("series").asText());
    assertEquals("max_path", out.get("transform").asText());
    assertEquals("2026-09-30", out.get("last_period").asText());
    assertEquals("2026-10-14", out.get("settlement_period").asText());
    assertEquals(10, out.get("steps_ahead").asInt());
    assertEquals(20, out.get("n").asInt());
    assertEquals(101.505, out.get("median").asDouble(), EPS);
    assertEquals(101.0, out.get("p05").asDouble(), EPS);
    assertEquals(102.01, out.get("p95").asDouble(), EPS);
    assertEquals("dollars", out.get("units").asText());
    assertEquals(java.util.Collections.singletonList("rounding_unverified"), flagCodes(out));
  }

  @Test void maxPathCannotFallBelowWhatTheWindowAlreadyReached() throws Exception {
    JsonNode out = run(oilVenue(), oil(22, 120), ",'window_start':'2026-09-20'");
    assertTrue(out.get("p05").asDouble() >= 120);
    assertTrue(out.get("method").asText().contains("floored at the highest row since "
        + "2026-09-20"));
  }

  // ─── as_of ─────────────────────────────────────────────────────────────────

  private static JsonNode asOfRun(String asOf) throws Exception {
    double[] v = unemployment();
    // A print far from every other: if it is used, the distribution shows it.
    v[19] = 99.0;
    FakeSql sql = new FakeSql().on("LNS14000000", blsRows(YearMonth.of(2025, 1), v));
    JsonNode out = run(venue("Unemployment rate in September 2026", UNEMPLOYMENT_RULES,
        CLOSE, 4.0), sql, ",'as_of':'" + asOf + "'");
    assertTrue(sql.queries.get(0).contains("\"year\" <= 2026"));
    return out;
  }

  @Test void asOfDropsRowsWhoseReleaseHasNotPrinted() throws Exception {
    // The unemployment rate prints within 10 days of its month's end.
    JsonNode july = asOfRun("2026-08-10");
    assertEquals("2026-07", july.get("last_period").asText());
    assertEquals("2026-08-10", july.get("as_of").asText());
    assertEquals(2, july.get("steps_ahead").asInt());
    assertEquals(17, july.get("n").asInt());
    assertTrue(july.get("p95").asDouble() < 5);
    assertTrue(flagCodes(july).contains("last_period_before_settlement"));

    // July had ended on the 31st, but its release had not printed.
    assertEquals("2026-06", asOfRun("2026-07-31").get("last_period").asText());
    // August's has not printed on September 9, so its row is not yet known.
    assertEquals("2026-07", asOfRun("2026-09-09").get("last_period").asText());
    // On the 10th it is.
    JsonNode august = asOfRun("2026-09-10");
    assertEquals("2026-08", august.get("last_period").asText());
    assertTrue(august.get("p95").asDouble() > 90);
  }

  @Test void asOfIncludesADailyRowDatedOnIt() throws Exception {
    JsonNode out = run(oilVenue(), oil(), ",'as_of':'2026-09-29'");
    assertEquals("2026-09-29", out.get("last_period").asText());
    JsonNode before = run(oilVenue(), oil(), ",'as_of':'2026-09-28'");
    assertEquals("2026-09-28", before.get("last_period").asText());
  }

  @Test void asOfMustBeADate() {
    assertThrows(IllegalArgumentException.class, () -> unemploymentRun(",'as_of':'Sept 1'"));
  }

  // ─── Flags ─────────────────────────────────────────────────────────────────

  @Test void aSeasonalMismatchIsFlaggedWhicheverWayItRuns() throws Exception {
    // Rules say adjusted; the series named is not.
    double[] v = new double[128];
    for (int t = 0; t < v.length; t++) {
      v[t] = 100 * Math.pow(1.002, t);
    }
    FakeSql nsa = new FakeSql().on("CUUR0000SA0", blsRows(YearMonth.of(2016, 1), v));
    JsonNode out = run(venue("CPI inflation in September 2026",
        "BLS CPI-U month-over-month change, seasonally adjusted, one decimal, in percent.",
        CLOSE, 0.3), nsa, ",'series':'CUUR0000SA0'");
    assertEquals(java.util.Collections.singletonList("seasonal_adjustment_mismatch"),
        flagCodes(out));
    assertTrue(out.get("flags").get(0).get("message").asText().contains("not seasonally"));

    // Rules say unadjusted; the headline rate exists only adjusted.
    FakeSql sa = new FakeSql().on("LNS14000000",
        blsRows(YearMonth.of(2025, 1), unemployment()));
    out = run(venue("Unemployment rate in September 2026",
        "Unemployment rate, not seasonally adjusted, one decimal, in percent.", CLOSE, 4.0),
        sa, "");
    assertEquals(java.util.Collections.singletonList("seasonal_adjustment_mismatch"),
        flagCodes(out));
  }

  @Test void aLastPeriodBeforeThePeriodBeforeSettlementIsFlagged() throws Exception {
    // Settlement is October; the catalog stops in August, so September is missing.
    JsonNode out = unemploymentRun(",'settlement_period':'2026-10'");
    assertEquals(2, out.get("steps_ahead").asInt());
    assertEquals(java.util.Collections.singletonList("last_period_before_settlement"),
        flagCodes(out));
    String msg = out.get("flags").get(0).get("message").asText();
    assertTrue(msg.contains("2026-08") && msg.contains("2026-09"), msg);
  }

  @Test void roundingThatDiffersFromTheRulesIsFlaggedAndTheOverrideIsUsed() throws Exception {
    JsonNode out = unemploymentRun(",'round':2");
    assertEquals(2, out.get("round").asInt());
    assertEquals(java.util.Collections.singletonList("rounding_mismatch"), flagCodes(out));
    // Rounding the rules do not state, with none given, is flagged as unverified.
    FakeSql sql = new FakeSql().on("LNS14000000",
        blsRows(YearMonth.of(2025, 1), unemployment()));
    JsonNode silent = run(venue("Unemployment rate in September 2026",
        "Resolves on the BLS unemployment rate, seasonally adjusted, in percent.", CLOSE, 4.0),
        sql, "");
    assertEquals(java.util.Collections.singletonList("rounding_unverified"),
        flagCodes(silent));
    assertTrue(silent.get("round").isNull());
  }

  @Test void unitsThatDifferFromTheRulesAreFlagged() throws Exception {
    double[] v = new double[20];
    for (int t = 0; t < v.length; t++) {
      v[t] = 300 + t;
    }
    FakeSql sql = new FakeSql().on("CUUR0000SA0", blsRows(YearMonth.of(2025, 1), v));
    JsonNode out = run(venue("CPI inflation in September 2026",
        "Resolves on the change in CPI-U, in percent, one decimal.", CLOSE, 3.0), sql,
        ",'series':'CUUR0000SA0','transform':'level'");
    assertEquals("level", out.get("transform").asText());
    assertEquals("override", out.get("transform_source").asText());
    assertEquals("index", out.get("units").asText());
    assertEquals(java.util.Collections.singletonList("units_mismatch"), flagCodes(out));
  }

  @Test void aSeriesTheCatalogLacksReturnsTheFlagAndNoForecast() throws Exception {
    // The rules name the BLS average-price series (govdata-ops issue 851); no query is made.
    FakeSql none = new FakeSql();
    JsonNode out = run(venue("Egg prices in October 2026",
        "Resolves on BLS average price, eggs, Grade A, large, per dozen (APU0000708111), "
        + "in dollars.", CLOSE, 3.5), none, "");
    assertEquals("not_forecast", out.get("status").asText());
    assertEquals("APU0000708111", out.get("series_named").asText());
    assertTrue(out.get("series").isNull());
    assertEquals(java.util.Collections.singletonList("series_not_sourced"), flagCodes(out));
    assertFalse(out.has("median"));
    assertTrue(none.queries.isEmpty());

    // The same event with no id in its rules is resolved from its driver.
    JsonNode byDriver = run(venue("Price of dozen eggs in October 2026?",
        "Resolves on the average price of eggs.", CLOSE, 3.5), none, "");
    assertEquals(java.util.Collections.singletonList("series_not_sourced"),
        flagCodes(byDriver));
    assertTrue(none.queries.isEmpty());
  }

  @Test void aGapInAMonthlySeriesKeepsEveryPairWithBothEndpointsAndSaysSo()
      throws Exception {
    double[] all = unemployment();
    ArrayNode rows = MAPPER.createArrayNode();
    ArrayNode full = blsRows(YearMonth.of(2025, 1), all);
    for (int i = 0; i < full.size(); i++) {
      if (i != 4) {
        rows.add(full.get(i));
      }
    }
    FakeSql sql = new FakeSql().on("LNS14000000", rows);
    JsonNode out = run(venue("Unemployment rate in September 2026", UNEMPLOYMENT_RULES, CLOSE,
        4.0), sql, "");
    assertEquals("2025-01", out.get("history_start").asText());
    assertEquals(all.length - 1, out.get("history_rows").asInt());
    assertEquals(java.util.Collections.singletonList("history_gap"), flagCodes(out));
    assertTrue(out.get("flags").get(0).toString().contains("2025-05"), out.toString());
  }

  /** BLS never published October 2025; its row carries a null value. */
  private static ArrayNode cpiWithNullOctober(int months) {
    double[] v = new double[months];
    for (int t = 0; t < v.length; t++) {
      v[t] = 100 * Math.pow(1.0025, t);
    }
    ArrayNode rows = blsRows(YearMonth.of(2024, 1), v);
    ((ObjectNode) rows.get(21)).putNull("value");
    return rows;
  }

  @Test void aNullValueIsAMissingMonthAndYearOverYearUsesTheMonthsWithBothEndpoints()
      throws Exception {
    FakeSql sql = new FakeSql().on("CUUR0000SA0", cpiWithNullOctober(32));
    JsonNode out = run(venue("CPI inflation in September 2026",
        "BLS CPI-U 12-month change, not seasonally adjusted, one decimal, in percent.",
        CLOSE, 3.0), sql, "");
    assertEquals("yoy_pct", out.get("transform").asText());
    assertEquals(17, out.get("n").asInt());
    assertEquals(3.0, out.get("median").asDouble(), EPS);
    assertEquals(java.util.Collections.singletonList("history_gap"), flagCodes(out));
    assertTrue(out.get("flags").get(0).toString().contains("2025-10"), out.toString());
  }

  @Test void aSettlementValueThatNeedsTheMissingMonthFailsNamingIt() {
    // Latest row 2026-09 settles 2026-10, whose year-over-year needs the null 2025-10.
    FakeSql sql = new FakeSql().on("CUUR0000SA0", cpiWithNullOctober(33));
    String m = message(() -> run(venue("CPI inflation in October 2026",
        "BLS CPI-U 12-month change, not seasonally adjusted, one decimal, in percent.",
        "2026-11-09T12:30:00Z", 3.0), sql, ""));
    assertTrue(m.contains("2025-10"), m);
  }

  /** The KXCPI-26SEP rules text as the venue serves it. */
  private static final String KXCPI_RULES = "If the Consumer Price Index (CPI) increases by "
      + "more than -0.4% (single-decimal) in September 2026, then the market resolves to "
      + "Yes. The Expiration Value is the single-decimal value published at the Source "
      + "Agency.";

  @Test void increasesByMoreThanInAMonthIsTheOneMonthChangeRoundedToOneDecimal()
      throws Exception {
    double[] v = new double[40];
    for (int t = 0; t < v.length; t++) {
      v[t] = 100 * Math.pow(1.0025, t);
    }
    FakeSql sql = new FakeSql().on("CPIAUCSL", monthlyRows(YearMonth.of(2023, 5), v));
    JsonNode out = run(venue("CPI in September", KXCPI_RULES, CLOSE, -0.4), sql, "");
    assertEquals("CPIAUCSL", out.get("series").asText());
    assertEquals("mom_pct", out.get("transform").asText());
    // 0.25 percent a month, read to one decimal as the single-decimal rule says.
    assertEquals(0.2, out.get("median").asDouble(), EPS);
  }

  // ─── Units, staleness, core CPI, Treasury yields ───────────────────────────

  private static final String PAYROLL_RULES = "If the increase in total non-farm payroll "
      + "employment is above -25000 as reported by the Bureau of Labor Statistics Monthly "
      + "Employment Situation Report for the month of October 2026, then the market "
      + "resolves to Yes.";

  private static FakeSql payrollSql() {
    double[] v = new double[21];
    for (int t = 0; t < v.length; t++) {
      v[t] = 159000 + 100.0 * t + (t % 2 == 0 ? 0 : 50);
    }
    return new FakeSql().on("CES0000000001", blsRows(YearMonth.of(2025, 1), v));
  }

  /** The series is in thousands of jobs; KXPAYROLLS strikes are in jobs. */
  @Test void payrollSamplesComeOutInJobsWhenTheVenueCountsJobs() throws Exception {
    JsonNode out = run(venue("Jobs numbers in October 2026?", PAYROLL_RULES,
        "2026-11-06T12:30:00Z", -25000), payrollSql(), "");
    assertEquals("CES0000000001", out.get("series").asText());
    assertEquals("change", out.get("transform").asText());
    assertEquals("jobs", out.get("units").asText());
    assertEquals(50000, out.get("p05").asDouble(), EPS);
    assertEquals(150000, out.get("p95").asDouble(), EPS);
    assertEquals(java.util.Collections.singletonList("rounding_unverified"), flagCodes(out));
  }

  @Test void payrollRulesThatStateNoUnitsRaiseUnitsMismatch() throws Exception {
    JsonNode out = run(venue("Payroll change in October 2026", PAYROLL_RULES,
        "2026-11-06T12:30:00Z", -25000), payrollSql(), "");
    assertEquals("thousands", out.get("units").asText());
    assertTrue(flagCodes(out).contains("units_mismatch"), out.toString());
  }

  @Test void aStaleDailyRowRaisesHistoryStaleNamingLastPeriodAndAge() throws Exception {
    // Last row 2026-09-30; as_of 2026-10-10 is 10 days on, over the 5 allowed for daily.
    JsonNode out = run(oilVenue(), oil(), ",'as_of':'2026-10-10'");
    assertTrue(flagCodes(out).contains("history_stale"), out.toString());
    String m = out.get("flags").toString();
    assertTrue(m.contains("2026-09-30") && m.contains("10 days"), m);
  }

  @Test void aFreshDailyRowRaisesNoHistoryStale() throws Exception {
    JsonNode out = run(oilVenue(), oil(), "");
    assertFalse(flagCodes(out).contains("history_stale"), out.toString());
  }

  @Test void aMonthlyRowIsStaleOnceTheFollowingMonthEndedOverFortyFiveDaysAgo()
      throws Exception {
    // Rows end 2026-08, so September ended 2026-09-30; 2026-12-01 is 62 days on.
    JsonNode out = unemploymentRun(",'as_of':'2026-12-01'");
    assertTrue(flagCodes(out).contains("history_stale"), out.toString());
    assertTrue(out.get("flags").toString().contains("62 days"), out.toString());
    assertFalse(flagCodes(unemploymentRun("")).contains("history_stale"));
  }

  private static final String KXCPICORE_RULES = "If the seasonally adjusted Consumer Price "
      + "Index for All Urban Consumers: All Items less Food and Energy for September 2026, "
      + "as published by the Bureau of Labor Statistics, increases by above 0.0%, then the "
      + "market resolves to Yes. Please note that the value of the Underlying is the "
      + "single-decimal value reported by the BLS.";

  @Test void coreCpiIncreasesByAboveIsTheOneMonthChangeOnTheAdjustedCoreSeries()
      throws Exception {
    double[] v = new double[40];
    for (int t = 0; t < v.length; t++) {
      v[t] = 100 * Math.pow(1.0025, t);
    }
    FakeSql sql = new FakeSql().on("CPILFESL", monthlyRows(YearMonth.of(2023, 5), v));
    JsonNode out = run(venue("CPI core in September", KXCPICORE_RULES, CLOSE, 0.0), sql, "");
    assertEquals("CPILFESL", out.get("series").asText());
    assertEquals("mom_pct", out.get("transform").asText());
    assertEquals("percent", out.get("units").asText());
    assertEquals(0.2, out.get("median").asDouble(), EPS);
  }

  private static final String TREASURY_CLOSE = "2026-10-30T20:00:00Z";

  private static String treasuryRules(String direction, String tenor) {
    return "If the daily published par yield for the " + tenor + " U.S. Treasury is "
        + direction + " 5.29% on any business day between Oct 1, 2026 and Oct 30, 2026, "
        + "then the market resolves to Yes.";
  }

  private static FakeSql yieldSql(String series) {
    double[] v = new double[60];
    for (int i = 0; i < v.length; i++) {
      v[i] = 4.9 + 0.01 * (i % 5);
    }
    return new FakeSql().on(series, dailyRows(LocalDate.of(2026, 7, 26), v));
  }

  @Test void howHighTheTenYearYieldGetsIsAMaxPathOnDgs10() throws Exception {
    JsonNode out = run(venue("How high will the 10-year US Treasury yield get by Oct 30, 2026?",
        treasuryRules("above", "10-year"), TREASURY_CLOSE, 5.29), yieldSql("DGS10"), "");
    assertEquals("DGS10", out.get("series").asText());
    assertEquals("max_path", out.get("transform").asText());
    assertEquals("2026-09-23", out.get("last_period").asText());
    assertTrue(out.get("p05").asDouble() >= 4.93, out.toString());
    assertTrue(flagCodes(out).contains("history_stale"), out.toString());
  }

  @Test void howLowTheThirtyYearYieldGetsIsAMinPathOnDgs30() throws Exception {
    JsonNode out = run(venue("How low will the 30-year US Treasury yield get by Oct 30, 2026?",
        treasuryRules("below", "30-year"), TREASURY_CLOSE, 5.29), yieldSql("DGS30"), "");
    assertEquals("DGS30", out.get("series").asText());
    assertEquals("min_path", out.get("transform").asText());
    assertTrue(out.get("p95").asDouble() <= 4.94 + EPS, out.toString());
    assertTrue(out.get("method").asText().contains("lowest"), out.toString());
  }

  @Test void aTenorTheCatalogDoesNotCarryIsNotSourcedNotGuessed() throws Exception {
    JsonNode out = run(venue("How high will the 7-year US Treasury yield get by Oct 30, 2026?",
        treasuryRules("above", "7-year"), TREASURY_CLOSE, 5.29), new FakeSql(), "");
    assertEquals("not_forecast", out.get("status").asText());
    assertEquals("DGS7", out.get("series_named").asText());
    assertEquals(java.util.Collections.singletonList("series_not_sourced"), flagCodes(out));
  }

  // ─── Overrides ─────────────────────────────────────────────────────────────

  @Test void aTableAndSeriesTheBuilderDoesNotKnowCanBeNamed() throws Exception {
    double[] v = new double[12];
    for (int t = 0; t < v.length; t++) {
      v[t] = 10 + t % 4;
    }
    FakeSql sql = new FakeSql().on("econ.custom_table", monthlyRows(YearMonth.of(2025, 9), v));
    JsonNode out = unemploymentRunWith(sql, ",'table':'econ.custom_table','series':'ABC',"
        + "'date_column':'obs_date','value_column':'val','frequency':'monthly',"
        + "'change_mode':'additive','transform':'level','settlement_period':'2026-09',"
        + "'seasonally_adjusted':true,'round':1");
    assertEquals("ABC", out.get("series").asText());
    assertEquals("econ.custom_table", out.get("table").asText());
    assertEquals(11, out.get("n").asInt());
    assertTrue(out.get("seasonally_adjusted").asBoolean());
    String q = sql.queries.get(0);
    assertTrue(q.contains("\"obs_date\" AS \"date\"") && q.contains("\"val\" AS \"value\"")
        && q.contains("\"series\" = 'ABC'"), q);
    assertEquals(0, out.get("flags").size());
  }

  private static JsonNode unemploymentRunWith(FakeSql sql, String extra) throws Exception {
    return run(venue("Unemployment rate in September 2026", UNEMPLOYMENT_RULES, CLOSE, 4.0),
        sql, extra);
  }

  @Test void seriesSqlReplacesTheTableAndIsRunAsGiven() throws Exception {
    double[] v = new double[12];
    for (int t = 0; t < v.length; t++) {
      v[t] = 100 + 3 * t + (t % 2);
    }
    String query = "SELECT d AS date, v AS value FROM mine";
    FakeSql sql = new FakeSql().on("FROM mine", monthlyRows(YearMonth.of(2025, 9), v));
    JsonNode out = unemploymentRunWith(sql, ",'series_sql':'" + query + "',"
        + "'frequency':'monthly','transform':'change','settlement_period':'2026-09',"
        + "'round':0");
    assertEquals(query, sql.queries.get(0));
    assertEquals("series_sql", out.get("series").asText());
    assertTrue(out.get("table").isNull());
    assertEquals(11, out.get("n").asInt());
    assertEquals("change", out.get("transform").asText());
  }

  @Test void anOverrideThatLeavesAPartOutNamesIt() {
    FakeSql none = new FakeSql();
    String m = message(() -> unemploymentRunWith(none,
        ",'table':'econ.t','series':'ZZZ'"));
    assertTrue(m.contains("date_column") && m.contains("value_column")
        && m.contains("frequency"), m);
    m = message(() -> unemploymentRunWith(none, ",'series':'ZZZ'"));
    assertTrue(m.contains("name its table"), m);
    m = message(() -> unemploymentRunWith(none, ",'table':'econ.t'"));
    assertTrue(m.contains("table needs series"), m);
    m = message(() -> unemploymentRunWith(none, ",'series_sql':'SELECT 1'"));
    assertTrue(m.contains("frequency is missing"), m);
    m = message(() -> unemploymentRunWith(none,
        ",'series_sql':'SELECT 1','series':'X','frequency':'monthly'"));
    assertTrue(m.contains("not both"), m);
    m = message(() -> unemploymentRunWith(none, ",'table':'econ.t; DROP','series':'X'"));
    assertTrue(m.contains("schema.table"), m);
    assertTrue(none.queries.isEmpty());
  }

  @Test void levelOnASeriesOfUnknownKindNeedsChangeMode() {
    FakeSql sql = new FakeSql().on("econ.custom_table",
        monthlyRows(YearMonth.of(2025, 1), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12));
    String m = message(() -> unemploymentRunWith(sql,
        ",'table':'econ.custom_table','series':'ABC','date_column':'d','value_column':'v',"
        + "'frequency':'monthly','transform':'level','settlement_period':'2026-09'"));
    assertTrue(m.contains("change_mode is missing"), m);
  }

  // ─── A spec that cannot be resolved ────────────────────────────────────────

  private interface Call {
    void run() throws Exception;
  }

  private static String message(Call c) {
    try {
      c.run();
    } catch (Exception e) {
      return e.getMessage();
    }
    throw new AssertionError("expected an error");
  }

  @Test void anUnresolvableSpecFailsNamingWhatIsMissing() {
    FakeSql none = new FakeSql();
    String m = message(() -> run(venue("Will the Lakers win?", "Resolves on the score.",
        CLOSE, 1), none, ""));
    assertTrue(m.contains("matches no driver"), m);
    m = message(() -> run(venue("Effective tariff rate in October 2026",
        "Resolves on the effective tariff rate.", CLOSE, 10), none, ""));
    assertTrue(m.contains("tariffs") && m.contains("series is missing"), m);
    m = message(() -> run(venue("CPI shelter inflation in September 2026",
        "Month-over-month change in the CPI shelter index.", CLOSE, 0.3), none, ""));
    assertTrue(m.contains("CPI component"), m);
    m = message(() -> run(venue("Black unemployment rate in September 2026",
        "BLS unemployment rate for Black workers.", CLOSE, 6), none, ""));
    assertTrue(m.contains("demographic"), m);
    assertTrue(none.queries.isEmpty());
  }

  @Test void aTransformTheRulesDoNotNameIsMissingNotGuessed() {
    FakeSql none = new FakeSql();
    String m = message(() -> run(venue("CPI inflation in September 2026",
        "Resolves on the BLS CPI-U, seasonally adjusted, one decimal.", CLOSE, 3.0), none,
        ""));
    assertTrue(m.contains("transform is missing") && m.contains("CPIAUCSL"), m);
    // Both a monthly and a 12-month change in the rules is not a choice the builder makes.
    m = message(() -> run(venue("CPI inflation in September 2026",
        "The month-over-month change and the 12-month change are both published.", CLOSE,
        3.0), none, ""));
    assertTrue(m.contains("more than one transform"), m);
  }

  @Test void aSettlementPeriodTheTextDoesNotNameIsMissing() {
    FakeSql sql = new FakeSql().on("CPIAUCSL",
        monthlyRows(YearMonth.of(2024, 8), 100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
            110, 111, 112));
    String m = message(() -> run(venue("CPI inflation next month",
        "Month-over-month change, seasonally adjusted, one decimal.", CLOSE, 0.3), sql, ""));
    assertTrue(m.contains("settlement period is missing"), m);
  }

  @Test void aPeriodAlreadyInTheCatalogIsNotForecast() {
    String m = message(() -> unemploymentRun(",'settlement_period':'2026-08'"));
    assertTrue(m.contains("already has 2026-08"), m);
  }

  @Test void tooFewSamplesAreAnErrorNotAThinForecast() {
    FakeSql sql = new FakeSql().on("LNS14000000",
        blsRows(YearMonth.of(2026, 1), 4.0, 4.1, 4.2, 4.1, 4.0, 4.1, 4.2, 4.3));
    String m = message(() -> unemploymentRunWith(sql, ""));
    assertTrue(m.contains("samples can be built"), m);
  }

  // ─── Arguments ─────────────────────────────────────────────────────────────

  @Test void unknownArgumentsAreRejectedBeforeAnythingIsRead() {
    FakeSql none = new FakeSql();
    String m = message(() -> new MarketForecasts(new FakeFetcher(), none, () -> NOW)
        .forecastMarketEvent(args("{'source':'kalshi','event_id':'x','bogus':1}")));
    assertTrue(m.contains("unknown key 'bogus'") && m.contains("allowed"), m);
    m = message(() -> new MarketForecasts(new FakeFetcher(), none, () -> NOW)
        .forecastMarketEvent(args("{'source':'kalshi'}")));
    assertTrue(m.contains("source and event_id are required"), m);
  }

  @Test void argumentsOfTheWrongKindAreRejected() {
    for (String bad : new String[]{",'round':-1", ",'round':'one'", ",'years':0",
        ",'transform':'sideways'", ",'frequency':'hourly'", ",'change_mode':'log'",
        ",'seasonally_adjusted':'yes'"}) {
      assertThrows(IllegalArgumentException.class, () -> unemploymentRun(bad), bad);
    }
  }

  @Test void theToolDefinitionAdvertisesExactlyTheArgumentsItAccepts() {
    ObjectNode def = MarketForecasts.toolDef();
    assertEquals("forecast_market_event", def.get("name").asText());
    assertFalse(def.get("description").asText().isEmpty());
    JsonNode schema = def.get("inputSchema");
    assertEquals("object", schema.get("type").asText());
    List<String> props = new ArrayList<>();
    schema.get("properties").fieldNames().forEachRemaining(props::add);
    assertEquals(new java.util.TreeSet<>(MarketForecasts.KEYS), new java.util.TreeSet<>(props));
    assertEquals("source", schema.get("required").get(0).asText());
    assertEquals("event_id", schema.get("required").get(1).asText());
  }

  // ─── Rules text ────────────────────────────────────────────────────────────

  @Test void rulesStateAdjustmentAndRounding() {
    assertEquals(Boolean.TRUE, MarketForecasts.rulesSeasonal("seasonally adjusted"));
    assertEquals(Boolean.FALSE, MarketForecasts.rulesSeasonal("not seasonally adjusted"));
    assertEquals(Boolean.FALSE, MarketForecasts.rulesSeasonal("the unadjusted index"));
    assertNull(MarketForecasts.rulesSeasonal("the index"));
    assertEquals(Integer.valueOf(1), MarketForecasts.rulesRound("rounded to one decimal"));
    assertEquals(Integer.valueOf(2), MarketForecasts.rulesRound("to 2 decimal places"));
    assertEquals(Integer.valueOf(1), MarketForecasts.rulesRound("nearest tenth of a percent"));
    assertEquals(Integer.valueOf(2), MarketForecasts.rulesRound("nearest 0.01"));
    assertEquals(Integer.valueOf(0), MarketForecasts.rulesRound("nearest whole number"));
    assertNull(MarketForecasts.rulesRound("as published"));
  }

  // ─── Pricing ───────────────────────────────────────────────────────────────

  /**
   * The forecast of the first test, handed to MarketPricing: 13 of 19 samples are 4.2 and the
   * market settles above 4.0, so fair is 13/19. The samples and round in the report give the
   * same forecast when passed to price_market_event.
   */
  @Test void theForecastPricesInMarketPricing() throws Exception {
    FakeFetcher f = venue("Unemployment rate in September 2026", UNEMPLOYMENT_RULES, CLOSE,
        4.0);
    FakeSql sql = new FakeSql().on("LNS14000000",
        blsRows(YearMonth.of(2025, 1), unemployment()));
    MarketForecasts mf = new MarketForecasts(f, sql, () -> NOW);
    PredictionMarkets.LiveEvent live = PredictionMarkets.fetchEvent(f, "kalshi", ID);
    MarketForecasts.Result r = mf.forecast(live.event.eventTitle, live.event.rules,
        live.event.driver, live.event.closeTime,
        MarketForecasts.Request.parse(args("{'source':'kalshi','event_id':'" + ID + "'}")));
    assertNotNull(r.forecast);
    assertTrue(r.forecast.sampled);
    MarketPricing.Priced priced = MarketPricing.priceEvent(live, r.forecast,
        new LinkedHashMap<>(), 0.03, null, false, null);
    JsonNode market = priced.json.get("priced_markets").get(0);
    assertEquals(13.0 / 19, market.get("fair").asDouble(), 1e-4);
    assertEquals(19, priced.json.get("forecast").get("n").asInt());

    double[] raw = new double[r.json.get("samples").size()];
    for (int i = 0; i < raw.length; i++) {
      raw[i] = r.json.get("samples").get(i).asDouble();
    }
    MarketPricing.Forecast again = MarketPricing.samples(raw)
        .rounded(r.json.get("round").asInt());
    MarketPricing.Priced second = MarketPricing.priceEvent(live, again,
        new LinkedHashMap<>(), 0.03, null, false, null);
    assertEquals(market.get("fair").asDouble(),
        second.json.get("priced_markets").get(0).get("fair").asDouble(), 0.0);
  }

  // ─── Catalog ───────────────────────────────────────────────────────────────

  @Test void everySeriesTheBuilderResolvesIsInTheSchemaYaml() throws Exception {
    Map<String, String> yamls = new LinkedHashMap<>();
    for (MarketForecasts.Series s : MarketForecasts.CATALOG) {
      String[] parts = s.table.split("\\.");
      String yaml = yamls.get(parts[0]);
      if (yaml == null) {
        String resource = "/" + parts[0] + "/" + parts[0] + "-schema.yaml";
        try (InputStream in = MarketForecastsTest.class.getResourceAsStream(resource)) {
          assertNotNull(in, "no " + resource);
          yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        yamls.put(parts[0], yaml);
      }
      assertTrue(Pattern.compile("(?m)^\\s*-\\s+name:\\s+" + Pattern.quote(parts[1])
          + "\\s*$").matcher(yaml).find(), s.table + " is not in the schema");
      assertTrue(Pattern.compile("\\b" + Pattern.quote(s.id) + "\\b").matcher(yaml).find(),
          s.id + " is not named in the schema");
    }
  }
}
