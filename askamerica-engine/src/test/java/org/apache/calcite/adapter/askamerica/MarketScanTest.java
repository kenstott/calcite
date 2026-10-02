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
import java.time.Duration;
import java.time.Instant;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The scan against canned venue responses and a canned CPI series: no network, no catalog.
 *
 * <p>The fixture is October CPI on both venues. CPI rises a steady 0.25% a month, so every
 * historical change in the 12-month rate is zero and the forecast is 3.0 exactly: Kalshi's
 * "above 3.0" at a 0.40 bid is a No worth 1 that costs 0.60. The Polymarket market carries no
 * strike the engine can read, so it is left out with the reason.
 */
@Tag("unit")
class MarketScanTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-10-02T00:00:00Z");
  private static final String KALSHI_ID = "KXCPI-26OCT";
  private static final String POLY_ID = "9001";

  /** Answers a URL from the first registered prefix it starts with. */
  private static final class FakeFetcher implements PredictionMarkets.Fetcher {
    final Map<String, JsonNode> byPrefix = new LinkedHashMap<>();
    final Set<String> failing = new HashSet<>();
    CountDownLatch gate;

    @Override public JsonNode get(String url) throws IOException {
      if (gate != null) {
        try {
          gate.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }
      for (String f : failing) {
        if (url.startsWith(f)) {
          throw new IOException("unreachable: " + url);
        }
      }
      for (Map.Entry<String, JsonNode> e : byPrefix.entrySet()) {
        if (url.startsWith(e.getKey())) {
          return e.getValue();
        }
      }
      throw new IOException("no canned response for " + url);
    }
  }

  private static ObjectNode kalshiMarket(String ticker, double strike, String bid,
      String ask) {
    ObjectNode m = MAPPER.createObjectNode();
    m.put("ticker", ticker);
    m.put("status", "active");
    m.put("title", "Will CPI rise more than " + strike + "% in October 2026?");
    m.put("yes_bid_dollars", bid);
    m.put("yes_ask_dollars", ask);
    m.put("last_price_dollars", bid);
    m.put("volume_fp", "5000.00");
    m.put("volume_24h_fp", "800.00");
    m.put("open_interest_fp", "2000.00");
    m.put("close_time", "2026-11-12T13:30:00Z");
    m.put("strike_type", "greater");
    m.put("floor_strike", strike);
    m.put("rules_primary", "Resolves on the BLS CPI-U 12-month change, one decimal.");
    return m;
  }

  private static ObjectNode kalshiEvent() {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("title", "CPI inflation in October 2026");
    ev.put("event_ticker", KALSHI_ID);
    ev.put("series_ticker", "KXCPI");
    ev.put("category", "Economics");
    ev.putArray("settlement_sources").addObject().put("name", "BLS");
    ArrayNode markets = ev.putArray("markets");
    // A strike priced above one half, so the quotes bracket an implied median.
    markets.add(kalshiMarket(KALSHI_ID + "-T2.8", 2.8, "0.7000", "0.7400"));
    markets.add(kalshiMarket(KALSHI_ID + "-T3.0", 3.0, "0.4000", "0.4400"));
    markets.add(kalshiMarket(KALSHI_ID + "-T3.2", 3.2, "0.2000", "0.2400"));
    return ev;
  }

  private static ObjectNode polymarketEvent() {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("id", POLY_ID);
    ev.put("title", "US CPI inflation October 2026");
    ev.put("slug", "us-cpi-october-2026");
    ev.putArray("tags").addObject().put("label", "Economy");
    ObjectNode m = ev.putArray("markets").addObject();
    m.put("id", "m1");
    m.put("question", "Will US CPI be above 3.0% in October?");
    m.put("active", true);
    m.put("closed", false);
    m.put("outcomes", "[\"Yes\",\"No\"]");
    m.put("outcomePrices", "[\"0.55\",\"0.45\"]");
    m.put("bestBid", 0.54);
    m.put("bestAsk", 0.56);
    m.put("volumeNum", 20000);
    m.put("volume24hr", 3000);
    m.put("endDate", "2026-11-12T12:00:00Z");
    m.put("feesEnabled", false);
    m.put("description", "Resolves on BLS CPI-U year over year, one decimal.");
    return ev;
  }

  private static FakeFetcher venues() {
    FakeFetcher f = new FakeFetcher();
    ObjectNode series = MAPPER.createObjectNode();
    series.putObject("series").put("fee_type", "quadratic").put("fee_multiplier", 1);
    f.byPrefix.put(PredictionMarkets.KALSHI + "/series/KXCPI", series);
    ObjectNode one = MAPPER.createObjectNode();
    one.set("event", kalshiEvent());
    f.byPrefix.put(PredictionMarkets.KALSHI + "/events/" + KALSHI_ID, one);
    ObjectNode kList = MAPPER.createObjectNode();
    kList.putArray("events").add(kalshiEvent());
    kList.put("cursor", "");
    f.byPrefix.put(PredictionMarkets.KALSHI + "/events?", kList);
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/" + POLY_ID, polymarketEvent());
    ObjectNode pList = MAPPER.createObjectNode();
    pList.putArray("events").add(polymarketEvent());
    pList.put("next_cursor", "");
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/keyset?", pList);
    return f;
  }

  private static ArrayNode cpiRows() {
    return cpiRows(32);
  }

  /** Monthly index rows from 2024-01; 32 months end at 2026-08, the latest print at NOW. */
  private static ArrayNode cpiRows(int months) {
    ArrayNode rows = MAPPER.createArrayNode();
    YearMonth start = YearMonth.of(2024, 1);
    for (int i = 0; i < months; i++) {
      YearMonth ym = start.plusMonths(i);
      ObjectNode r = rows.addObject();
      r.put("year", ym.getYear());
      r.put("period", String.format("M%02d", ym.getMonthValue()));
      r.put("value", 100 * Math.pow(1.0025, i));
    }
    return rows;
  }

  private final List<String> queries = new ArrayList<>();

  private MarketScan scan(long budgetMillis) {
    return scan(budgetMillis, 32);
  }

  private MarketScan scan(long budgetMillis, int months) {
    FakeFetcher f = venues();
    return new MarketScan(f, new PredictionMarkets.ListingCache(f, Duration.ofMinutes(15)),
        (q, limit) -> {
          queries.add(q);
          return cpiRows(months);
        }, () -> NOW, 10_000L, budgetMillis, Duration.ofMinutes(15));
  }

  private static ObjectNode backtestReport(String series, String verdict) {
    ObjectNode r = MAPPER.createObjectNode();
    r.put("source", "kalshi").put("series", series).put("verdict", verdict)
        .put("verdict_reason", "r").put("events_scored", 9).put("days_before", 7)
        .put("brier_baseline", 0.05).put("brier_price", 0.09).put("brier_difference", -0.04)
        .put("brier_difference_se", 0.01);
    return r;
  }

  @Test void anOpportunityIsGradedByItsSeriesBacktestAndLeftOutWhenThePriceWon()
      throws Exception {
    FakeFetcher f = venues();
    MarketTools.SqlRunner sql = (q, limit) -> cpiRows(32);
    MarketBacktest backtest = new MarketBacktest(f, sql, () -> NOW);
    MarketScan s = new MarketScan(f,
        new PredictionMarkets.ListingCache(f, Duration.ofMinutes(15)), sql, () -> NOW, 10_000L,
        60_000L, Duration.ofMinutes(15), backtest);

    JsonNode first = MAPPER.readTree(s.scan(args("{'within':90}")));
    JsonNode kalshi = null;
    for (JsonNode o : first.get("opportunities")) {
      if ("kalshi".equals(o.get("source").asText())) {
        kalshi = o;
      }
    }
    assertNotNull(kalshi, first.toString());
    int passed = first.get("funnel").get("events_passed").asInt();
    String series = kalshi.get("venue_series").asText();
    assertEquals("weak", kalshi.get("confidence").get("tier").asText());
    assertTrue(kalshi.get("confidence").get("backtest").isNull());
    assertTrue(first.get("next").asText().contains(
        "you MUST call backtest_market_forecast(source, series=venue_series)"),
        first.get("next").asText());

    backtest.remember(backtestReport(series, "baseline_beats_market"));
    JsonNode graded = MAPPER.readTree(s.scan(args("{'within':90}")));
    JsonNode top = graded.get("opportunities").get(0);
    assertEquals(kalshi.get("event_id").asText(), top.get("event_id").asText(),
        "a backtested series ranks first");
    assertEquals("backtested", top.get("confidence").get("tier").asText(), top.toString());
    assertEquals(passed, graded.get("funnel").get("events_passed").asInt());

    backtest.remember(backtestReport(series, "market_beats_baseline"));
    JsonNode beaten = MAPPER.readTree(s.scan(args("{'within':90}")));
    String leftOut = "events_past_min_edge_left_out_as_the_price_beat_the_baseline_in_backtest";
    assertEquals(1, beaten.get("funnel").get(leftOut).asInt(), beaten.toString());
    assertEquals(passed - 1, beaten.get("funnel").get("events_passed").asInt());
    for (JsonNode o : beaten.get("opportunities")) {
      assertFalse(kalshi.get("event_id").asText().equals(o.get("event_id").asText()));
    }

    JsonNode shown = MAPPER.readTree(s.scan(args("{'within':90,'include_flagged':true}")));
    assertEquals(passed, shown.get("funnel").get("events_passed").asInt());
  }

  private static JsonNode args(String json) throws Exception {
    return MAPPER.readTree(json.replace('\'', '"'));
  }

  @Test void aScanReturnsTheEventsPastMinEdgeWithTheirForecast() throws Exception {
    JsonNode out = MAPPER.readTree(scan(60_000L).scan(args("{'within':90}")));
    assertEquals("complete", out.get("status").asText(), out.toString());
    JsonNode funnel = out.get("funnel");
    assertEquals(2, funnel.get("events_matched").asInt(), out.toString());
    assertEquals(2, funnel.get("events_evaluated").asInt());
    assertEquals(1, funnel.get("events_forecast").asInt(), out.toString());
    assertEquals(1, funnel.get("events_passed").asInt(), out.toString());
    JsonNode opp = out.get("opportunities").get(0);
    assertEquals(KALSHI_ID, opp.get("event_id").asText());
    assertEquals("CUUR0000SA0", opp.get("forecast").get("series").asText());
    assertEquals("yoy_pct", opp.get("forecast").get("transform").asText());
    JsonNode best = opp.get("markets").get(0);
    assertEquals(KALSHI_ID + "-T3.0", best.get("market_id").asText(), opp.toString());
    assertEquals("no", best.get("side").asText());
    assertEquals(0.60, best.get("price").asDouble(), 1e-9);
    assertTrue(best.get("edge").asDouble() > 0.35, best.toString());
    assertEquals(41.6, opp.get("days_to_settlement").asDouble(), 1e-9);
    JsonNode not = out.get("not_forecast");
    assertEquals(1, not.size(), not.toString());
    assertEquals("polymarket:" + POLY_ID, not.get(0).get("examples").get(0).asText());
    assertTrue(out.get("next").asText().startsWith("Vet at most 3"), out.get("next").asText());
  }

  @Test void aFinishedScanCarriesItsBoardAndFollowUps() throws Exception {
    MarketPresentation view = new MarketPresentation();
    JsonNode out = MAPPER.readTree(view.scan(scan(60_000L).scan(args("{'within':90}"))));
    assertEquals("scan:1", out.get(MarketPresentation.LAYOUT_FIELD).asText());
    ObjectNode dash = MAPPER.createObjectNode();
    dash.put(MarketPresentation.LAYOUT, "scan:1");
    assertTrue(view.resolve(dash));
    List<DashboardLayout.Panel> panels = new ArrayList<>();
    for (JsonNode p : dash.get("panels")) {
      panels.add(McpServer.readPanel(p));
    }
    // Four stats, the funnel, the ranking and the edge-against-error scatter.
    assertEquals(7, panels.size(), out.get("dashboard_panels").toString());
    int[] size = DashboardLayout.defaultSize(panels, 4);
    assertTrue(DashboardLayout.compose(dash.get("title").asText(),
        dash.get("subtitle").asText(), dash.get("footnote").asText(), panels, 4, size[0],
        size[1]).toSvg().contains("<svg"));
    assertEquals("price_market_event", out.get("follow_ups").get(0).get("tool").asText());
    for (JsonNode f : out.get("follow_ups")) {
      MarketToolsTest.assertToolAccepts(f);
    }

    JsonNode empty = MAPPER.readTree(
        view.scan(scan(60_000L).scan(args("{'within':90,'min_edge':0.9}"))));
    assertEquals("scan:2", empty.get(MarketPresentation.LAYOUT_FIELD).asText());
    assertEquals(5, empty.get("dashboard_panels").size());
  }

  @Test void anUnfinishedScanHasNoBoardYet() throws Exception {
    MarketPresentation view = new MarketPresentation();
    JsonNode first = MAPPER.readTree(view.scan(scan(-1L).scan(args("{'within':90}"))));
    assertEquals("scanning", first.get("status").asText());
    assertFalse(first.has(MarketPresentation.LAYOUT_FIELD));
    assertFalse(first.has("follow_ups"));
    assertNull(view.gate(false));
  }

  @Test void aMinEdgeNothingReachesLeavesNoOpportunityAndSaysSo() throws Exception {
    JsonNode out = MAPPER.readTree(scan(60_000L).scan(args("{'within':90,'min_edge':0.9}")));
    assertEquals(0, out.get("opportunities").size());
    assertEquals(0, out.get("funnel").get("events_passed").asInt());
    assertEquals(1, out.get("funnel").get("events_forecast").asInt());
    assertTrue(out.get("next").asText().contains("Say none was found"));
  }

  @Test void aThinlyTradedMarketDoesNotCount() throws Exception {
    JsonNode out = MAPPER.readTree(scan(60_000L).scan(
        args("{'within':90,'min_market_volume':6000}")));
    assertEquals(1, out.get("funnel").get("events_forecast").asInt(), out.toString());
    assertEquals(0, out.get("funnel").get("events_passed").asInt(), out.toString());
    assertEquals(0, out.get("opportunities").size());
  }

  @Test void anOpportunitySaysWhetherTheMarketIsInsideTheBaselineRange() throws Exception {
    JsonNode out = MAPPER.readTree(scan(60_000L).scan(args("{'within':90}")));
    JsonNode opp = out.get("opportunities").get(0);
    assertTrue(opp.get("market_implied_median").isNumber(), opp.toString());
    // The forecast is 3.0 exactly, so any other implied median is outside its range.
    assertEquals(MarketScan.OUTSIDE, opp.get("baseline_vs_market").asText(), opp.toString());
    assertTrue(out.get("baseline_vs_market_is").asText().contains(MarketScan.INSIDE));
  }

  @Test void aForecastFromStaleHistoryIsLeftOutUnlessAskedFor() throws Exception {
    // Rows end at 2026-05: the following month ended 94 days before NOW.
    MarketScan stale = scan(60_000L, 29);
    JsonNode out = MAPPER.readTree(stale.scan(args("{'within':90}")));
    JsonNode funnel = out.get("funnel");
    assertEquals(1, funnel.get("events_forecast").asInt(), out.toString());
    assertEquals(0, funnel.get("events_passed").asInt(), out.toString());
    assertEquals(1, funnel.get("events_past_min_edge_left_out_for_a_blocking_flag").asInt(),
        out.toString());
    assertEquals(0, out.get("opportunities").size());

    JsonNode shown = MAPPER.readTree(stale.scan(args("{'within':90,'include_flagged':true}")));
    assertEquals(1, shown.get("opportunities").size(), shown.toString());
    assertTrue(shown.get("opportunities").get(0).toString().contains("history_stale"),
        shown.toString());
  }

  @Test void aScanOutOfTimeResumesWhereItStopped() throws Exception {
    MarketScan s = scan(-1L);
    JsonNode first = MAPPER.readTree(s.scan(args("{'within':90}")));
    assertEquals("scanning", first.get("status").asText());
    assertEquals(1, first.get("funnel").get("events_evaluated").asInt());
    assertTrue(first.get("next").asText().contains("You MUST call " + MarketScan.TOOL));
    JsonNode second = MAPPER.readTree(s.scan(args("{'within':90}")));
    assertEquals("complete", second.get("status").asText());
    assertEquals(2, second.get("funnel").get("events_evaluated").asInt());
  }

  @Test void anEvaluatedEventIsNotEvaluatedAgain() throws Exception {
    MarketScan s = scan(60_000L);
    s.scan(args("{'within':90}"));
    int after = queries.size();
    assertTrue(after > 0);
    s.scan(args("{'within':90,'min_edge':0.2}"));
    assertEquals(after, queries.size());
    s.scan(args("{'within':90,'refresh':true}"));
    assertTrue(queries.size() > after);
  }

  @Test void aDriverFilterKeepsOnlyItsEvents() throws Exception {
    JsonNode out = MAPPER.readTree(scan(60_000L).scan(
        args("{'within':90,'driver':'unemployment'}")));
    assertEquals(0, out.get("funnel").get("events_matched").asInt());
  }

  @Test void unknownArgumentsAndDriversAreRejected() {
    MarketScan s = scan(60_000L);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> s.scan(args("{'edge':0.1}")));
    assertTrue(e.getMessage().contains("unknown key 'edge'"), e.getMessage());
    assertThrows(IllegalArgumentException.class, () -> s.scan(args("{'driver':'nope'}")));
    assertThrows(IllegalArgumentException.class, () -> s.scan(args("{'limit':'five'}")));
  }

  @Test void theToolDefinitionAdvertisesExactlyTheArgumentsItAccepts() {
    ObjectNode def = MarketScan.toolDef();
    Set<String> advertised = new HashSet<>();
    def.get("inputSchema").get("properties").fieldNames().forEachRemaining(advertised::add);
    assertEquals(MarketScan.KEYS, advertised);
    assertTrue(def.get("description").asText().length() <= 2048);
  }
}
