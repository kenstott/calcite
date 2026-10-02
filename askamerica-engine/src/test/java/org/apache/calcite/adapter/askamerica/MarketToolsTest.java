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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The prediction-market tools against canned venue responses: no network, no catalog.
 *
 * <p>The fixture is one quantity, October CPI, quoted on both venues. Kalshi asks 0.44 for
 * "above 3.0" and Polymarket bids 0.54 for the same outcome, so buying Yes on one and No on
 * the other costs 0.917 after Kalshi's fee and pays 1 whatever CPI prints: a cross-venue lock.
 */
@Tag("unit")
class MarketToolsTest {

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

  private static MarketTools tools(PredictionMarkets.Fetcher fetcher,
      MarketTools.SqlRunner sql, long waitMillis) {
    return new MarketTools(fetcher,
        new PredictionMarkets.ListingCache(fetcher, Duration.ofMinutes(15)), sql, () -> NOW,
        waitMillis);
  }

  private static MarketTools tools() {
    return tools(venues(), (q, limit) -> {
      throw new AssertionError("no query expected: " + q);
    }, 10_000L);
  }

  private static JsonNode args(String json) throws Exception {
    return MAPPER.readTree(json.replace('\'', '"'));
  }

  private static JsonNode call(String out) throws Exception {
    return MAPPER.readTree(out);
  }

  private static String kalshiSpec(String extra) {
    return "{'source':'kalshi','event_id':'" + KALSHI_ID + "'" + extra + "}";
  }

  private static String polySpec(String extra) {
    return "{'source':'polymarket','event_id':'" + POLY_ID
        + "','conditions':{'m1':{'above':3.0}}" + extra + "}";
  }

  // ─── Drivers ───────────────────────────────────────────────────────────────

  @Test void driversMatchWhatTheCatalogMeasures() {
    assertEquals("inflation", PredictionMarkets.driverOf("Will CPI rise above 3%?").name);
    assertEquals("tariffs",
        PredictionMarkets.driverOf("US effective tariff rate on China above 30%?").name);
    assertNull(PredictionMarkets.driverOf("China CPI above 1%?"));
    assertNull(PredictionMarkets.driverOf("Who will the Fed nominee be?"));
    assertNull(PredictionMarkets.driverOf("Highest temperature in Toronto on Friday?"));
    assertEquals("temperature",
        PredictionMarkets.driverOf("Highest temperature in Chicago on Friday?").name);
  }

  @Test void everyDriverTableExistsInTheGovdataSchemas() throws Exception {
    Map<String, String> yamls = new LinkedHashMap<>();
    for (PredictionMarkets.Driver d : PredictionMarkets.DRIVERS) {
      assertTrue(PredictionMarkets.BASES.contains(d.basis), d.name);
      assertFalse(d.tables.isEmpty(), d.name);
      for (String qualified : d.tables) {
        String[] parts = qualified.split("\\.");
        assertEquals(2, parts.length, qualified);
        String yaml = yamls.get(parts[0]);
        if (yaml == null) {
          String resource = "/" + parts[0] + "/" + parts[0] + "-schema.yaml";
          try (InputStream in = MarketToolsTest.class.getResourceAsStream(resource)) {
            assertNotNull(in, "driver " + d.name + " names schema '" + parts[0]
                + "', which has no " + resource);
            yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
          }
          yamls.put(parts[0], yaml);
        }
        assertTrue(
            Pattern.compile("(?m)^\\s*-\\s+name:\\s+" + Pattern.quote(parts[1]) + "\\s*$")
                .matcher(yaml).find(),
            "driver " + d.name + " names " + qualified + ", which the schema does not define");
      }
    }
  }

  // ─── Both venues ───────────────────────────────────────────────────────────

  @Test void candidatesCoverBothVenues() throws Exception {
    JsonNode out = call(tools().findCandidates(args("{}")));
    assertEquals(2, out.get("matched").asInt());
    assertEquals(1, out.get("matched_by_venue").get("kalshi").asInt());
    assertEquals(1, out.get("matched_by_venue").get("polymarket").asInt());
    assertEquals(2, out.get("listing").get("kalshi_markets_read").asInt());
    assertEquals(1, out.get("listing").get("polymarket_markets_read").asInt());
    JsonNode first = out.get("events").get(0);
    assertEquals("inflation", first.get("driver").asText());
    assertEquals("release", first.get("basis").asText());
    assertTrue(first.get("forecast_with").asText().contains("arima_forecast"));
    assertFalse(out.has("venue_note"));
  }

  @Test void aRandomDrawAlternatesVenuesAndIsRepeatable() throws Exception {
    JsonNode out = call(tools().findCandidates(args("{'sample':2,'seed':7}")));
    assertEquals(7, out.get("seed").asLong());
    assertEquals(1, out.get("shown_by_venue").get("kalshi").asInt());
    assertEquals(1, out.get("shown_by_venue").get("polymarket").asInt());
    JsonNode unseeded = call(tools().findCandidates(args("{'sample':1}")));
    assertTrue(unseeded.has("seed"));
    assertEquals(1, unseeded.get("shown").asInt());
  }

  @Test void aVenueWithNothingToShowIsNamed() throws Exception {
    JsonNode out = call(tools().findCandidates(
        args("{'exclude':['" + KALSHI_ID + "']}")));
    assertEquals(0, out.get("matched_by_venue").get("kalshi").asInt());
    assertTrue(out.get("venue_note").asText().startsWith("kalshi lists no event"));
  }

  @Test void balancedTakesFromEachVenueInTurnAndKeepsTheOrder() {
    List<PredictionMarkets.Event> ordered = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      PredictionMarkets.Event e = new PredictionMarkets.Event();
      e.source = i < 5 ? "kalshi" : "polymarket";
      e.eventId = "e" + i;
      ordered.add(e);
    }
    List<PredictionMarkets.Event> four = MarketTools.balanced(ordered, 4);
    List<String> ids = new ArrayList<>();
    for (PredictionMarkets.Event e : four) {
      ids.add(e.eventId);
    }
    assertEquals(List.of("e0", "e1", "e5", "e6"), ids);
    assertEquals(3, MarketTools.balanced(ordered.subList(0, 3), 9).size());
  }

  @Test void candidateFiltersAreValidated() {
    assertThrows(IllegalArgumentException.class,
        () -> tools().findCandidates(args("{'driver':'cpi'}")));
    assertThrows(IllegalArgumentException.class,
        () -> tools().findCandidates(args("{'basis':'guess'}")));
    assertThrows(IllegalArgumentException.class,
        () -> tools().findCandidates(args("{'limit':'ten'}")));
  }

  @Test void aListingStillBeingReadSaysSo() throws Exception {
    FakeFetcher f = venues();
    f.gate = new CountDownLatch(1);
    MarketTools t = tools(f, (q, limit) -> MAPPER.createArrayNode(), 50L);
    try {
      JsonNode out = call(t.findCandidates(args("{}")));
      assertEquals("loading", out.get("status").asText());
      assertTrue(out.get("message").asText().contains("Call this tool again"));
    } finally {
      f.gate.countDown();
    }
  }

  @Test void oneVenueFailingFailsTheListing() {
    FakeFetcher f = venues();
    f.failing.add(PredictionMarkets.POLYMARKET + "/events/keyset?");
    MarketTools t = tools(f, (q, limit) -> MAPPER.createArrayNode(), 10_000L);
    assertThrows(IOException.class, () -> t.findCandidates(args("{}")));
  }

  // ─── Pricing one event ─────────────────────────────────────────────────────

  @Test void withNoForecastAnEventReturnsQuotesRulesAndItsCounterpart() throws Exception {
    JsonNode out = call(tools().priceEvent(args(kalshiSpec(""))));
    assertTrue(out.get("forecast").isNull());
    assertTrue(out.get("rules").asText().contains("CPI-U"));
    assertEquals(2, out.get("priced_markets").size());
    assertEquals("not_forecast", out.get("priced_markets").get(0).get("verdict").asText());
    assertTrue(out.get("next").asText().contains("arima_forecast"));
    assertTrue(out.get("next").asText().contains("MUST call describe_table"));
    JsonNode other = out.get("other_venue");
    assertEquals("polymarket", other.get("venue").asText());
    assertEquals("found", other.get("status").asText());
    assertEquals(POLY_ID, other.get("counterparts").get(0).get("event_id").asText());
  }

  @Test void aPolymarketEventPointsBackAtKalshi() throws Exception {
    JsonNode out = call(tools().priceEvent(
        args("{'source':'polymarket','event_id':'" + POLY_ID + "'}")));
    assertEquals(1, out.get("markets_without_condition").size());
    assertEquals("kalshi", out.get("other_venue").get("venue").asText());
    assertEquals(KALSHI_ID,
        out.get("other_venue").get("counterparts").get(0).get("event_id").asText());
  }

  @Test void anArimaForecastPricesEachMarketNetOfTheFee() throws Exception {
    JsonNode out = call(tools().priceEvent(
        args(kalshiSpec(",'mean':3.3,'sd':0.1,'min_edge':0.10"))));
    JsonNode m = out.get("priced_markets").get(0);
    assertEquals(KALSHI_ID + "-T3.0", m.get("market_id").asText());
    assertEquals("yes", m.get("side").asText());
    assertEquals(0.44, m.get("price").asDouble(), 1e-9);
    assertEquals(0.07 * 0.44 * 0.56, m.get("fee").asDouble(), 1e-5);
    assertEquals(0.9987, m.get("fair").asDouble(), 0.001);
    assertEquals(0.9987 - 0.44 - 0.07 * 0.44 * 0.56, m.get("edge").asDouble(), 0.001);
    assertEquals("mispriced", m.get("verdict").asText());
    assertEquals(2, out.get("mispriced_markets").asInt());
    // Both strikes trade under 0.50, so the quotes bracket no median to compare against.
    assertTrue(out.get("implied_median").isNull());
    assertFalse(out.has("next"));
  }

  @Test void aSearchIsNotOverAtTheFirstEventThatFails() throws Exception {
    MarketTools t = tools();
    // Fair values of 0.45 and 0.14 against quotes of 0.40/0.44 and 0.20/0.24: neither side
    // of either market clears 0.10.
    JsonNode first = call(t.priceEvent(
        args(kalshiSpec(",'mean':2.974,'sd':0.2066,'min_edge':0.10")))).get("search");
    assertEquals(1, first.get("events_forecast_and_priced").asInt());
    assertEquals(0, first.get("events_with_a_market_past_min_edge").asInt());
    assertEquals(KALSHI_ID, first.get("priced_event_ids").get(0).asText());
    String next = first.get("next").asText();
    assertTrue(next.startsWith("You MUST price one counterpart of kalshi:" + KALSHI_ID), next);
    assertTrue(next.contains("polymarket:" + POLY_ID), next);
    assertTrue(next.contains("You MUST read its rules first"), next);
    assertTrue(next.contains("you MUST NOT answer yet"), next);
    assertTrue(next.contains(MarketTools.MAX_DRAWS + " events"), next);

    JsonNode second = call(t.priceEvent(args(polySpec(",'mean':3.3,'sd':0.1,'min_edge':0.10"))))
        .get("search");
    assertEquals(2, second.get("events_forecast_and_priced").asInt());
    assertEquals(1, second.get("events_with_a_market_past_min_edge").asInt());
    assertFalse(second.get("next").asText().contains("MUST"), second.get("next").asText());

    assertFalse(call(t.priceEvent(args(kalshiSpec("")))).has("search"));
  }

  @Test void aForecastPricingReturnsItsChartPanel() throws Exception {
    MarketTools t = tools();
    assertFalse(t.pricedWithForecast());
    assertFalse(call(t.priceEvent(args(kalshiSpec("")))).has("chart_panel"));
    assertFalse(t.pricedWithForecast());

    JsonNode out = call(t.priceEvent(args(kalshiSpec(",'mean':3.0,'sd':0.1"))));
    assertTrue(t.pricedWithForecast());
    JsonNode panel = out.get("chart_panel");
    assertEquals("chart", panel.get("type").asText());
    assertEquals("bar", panel.get("chart_type").asText());
    assertEquals(2, panel.get("categories").size());
    assertEquals("Forecast fair value", panel.get("series").get(0).get("name").asText());
    assertEquals(out.get("priced_markets").get(0).get("fair").asDouble(),
        panel.get("series").get(0).get("values").get(0).asDouble(), 1e-9);
    assertEquals(0.44, panel.get("series").get(1).get("values").get(0).asDouble(), 1e-9);
    assertEquals(0.24, panel.get("series").get(1).get("values").get(1).asDouble(), 1e-9);
    assertTrue(out.get("chart_panel_use").asText().contains("dashboard.panels"));
  }

  @Test void aRandomSearchCannotReportUntilItIsFinished() throws Exception {
    MarketTools t = tools();
    String miss = ",'mean':2.974,'sd':0.2066,'min_edge':0.10";
    t.priceEvent(args(kalshiSpec(miss)));
    // Pricing a named event is not a search: nothing is owed.
    assertNull(t.searchGate());

    t.findCandidates(args("{'sample':1}"));
    String gate = t.searchGate();
    assertTrue(gate.contains("1 of " + MarketTools.MAX_DRAWS), gate);
    assertTrue(gate.contains("exclude=[" + KALSHI_ID + "]"), gate);
    assertTrue(gate.contains("polymarket:" + POLY_ID), gate);

    // The counterpart priced and still nothing past min_edge: the draw is still owed.
    t.priceEvent(args(polySpec(",'mean':3.02,'sd':0.2,'min_edge':0.10")));
    gate = t.searchGate();
    assertTrue(gate.contains("2 of " + MarketTools.MAX_DRAWS), gate);
    // A counterpart owes nothing back, and one counterpart settles what its event owed.
    assertFalse(gate.contains("counterpart"), gate);

    // One event passes: the search is finished.
    t.priceEvent(args(polySpec(",'mean':3.3,'sd':0.1,'min_edge':0.10")));
    assertNull(t.searchGate());

    // A published report starts the next search from nothing.
    t.resetSearch();
    assertNull(t.searchGate());
    t.findCandidates(args("{'sample':1}"));
    t.priceEvent(args(polySpec(",'mean':3.02,'sd':0.2,'min_edge':0.10")));
    assertTrue(t.searchGate().contains("1 of " + MarketTools.MAX_DRAWS), t.searchGate());
  }

  @Test void theIntervalAndBandFormsMatchTheirForecastTools() throws Exception {
    JsonNode bySd = call(tools().priceEvent(args(kalshiSpec(",'mean':3.1,'sd':0.1"))));
    JsonNode byInterval = call(tools().priceEvent(args(kalshiSpec(
        ",'mean':3.1,'lower':" + (3.1 - 1.959964 * 0.1) + ",'upper':"
        + (3.1 + 1.959964 * 0.1) + ",'level':0.95"))));
    for (int i = 0; i < 2; i++) {
      assertEquals(bySd.get("priced_markets").get(i).get("fair").asDouble(),
          byInterval.get("priced_markets").get(i).get("fair").asDouble(), 0.002);
    }
    assertEquals(0.84, bySd.get("priced_markets").get(0).get("fair").asDouble(), 0.01);
    JsonNode band = call(tools().priceEvent(
        args(kalshiSpec(",'price_median':3.1,'cumulative_vol_pct':5"))));
    assertEquals(3.1, band.get("forecast").get("median").asDouble(), 0.01);
    // ln(3.0 / 3.1) / 0.05 = -0.656, so P(above 3.0) = 0.744.
    assertEquals(0.744, band.get("priced_markets").get(0).get("fair").asDouble(), 0.01);
  }

  @Test void samplesAreAnEmpiricalDistributionRoundedAsTheRulesSettle() throws Exception {
    JsonNode out = call(tools().priceEvent(args(kalshiSpec(
        ",'samples':[2.96,3.04,3.14,3.26],'round':1"))));
    // Rounded to one decimal: 3.0, 3.0, 3.1, 3.3. Two of four are above 3.0.
    assertEquals(0.5, out.get("priced_markets").get(0).get("fair").asDouble(), 1e-9);
    assertEquals(0.25, out.get("priced_markets").get(1).get("fair").asDouble(), 1e-9);
    assertEquals(4, out.get("forecast").get("n").asInt());
  }

  @Test void samplesSqlReadsItsFirstColumnAndRefusesNullsAndTruncation() throws Exception {
    List<String> seen = new ArrayList<>();
    MarketTools.SqlRunner three = (q, limit) -> {
      seen.add(q + "|" + limit);
      ArrayNode rows = MAPPER.createArrayNode();
      for (double v : new double[]{2.9, 3.1, 3.3}) {
        rows.addObject().put("yoy", v).put("ignored", 99);
      }
      return rows;
    };
    JsonNode out = call(tools(venues(), three, 10_000L).priceEvent(
        args(kalshiSpec(",'samples_sql':'select yoy from t'"))));
    assertEquals(List.of("select yoy from t|" + MarketTools.MAX_ROWS), seen);
    assertEquals(2.0 / 3, out.get("priced_markets").get(0).get("fair").asDouble(), 1e-4);

    MarketTools.SqlRunner withNull = (q, limit) -> {
      ArrayNode rows = MAPPER.createArrayNode();
      rows.addObject().put("yoy", 3.1);
      rows.addObject().putNull("yoy");
      return rows;
    };
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> tools(venues(), withNull, 10_000L).priceEvent(
            args(kalshiSpec(",'samples_sql':'select yoy from t'"))));
    assertTrue(e.getMessage().contains("row 2"), e.getMessage());

    MarketTools.SqlRunner full = (q, limit) -> {
      ArrayNode rows = MAPPER.createArrayNode();
      for (int i = 0; i < limit; i++) {
        rows.addObject().put("yoy", 3.1);
      }
      return rows;
    };
    assertThrows(IllegalArgumentException.class,
        () -> tools(venues(), full, 10_000L).priceEvent(
            args(kalshiSpec(",'samples_sql':'select yoy from t'"))));
  }

  @Test void aForecastIsGivenInExactlyOneCompleteForm() {
    for (String bad : new String[]{
        ",'mean':3.1,'sd':0.1,'samples':[1,2,3]",
        ",'sd':0.1",
        ",'mean':3.1",
        ",'mean':3.1,'lower':2.9",
        ",'mean':3.1,'sd':0.1,'lower':2.9,'upper':3.3",
        ",'price_median':3.1",
        ",'cumulative_vol_pct':5",
        ",'samples':[3.1]",
        ",'samples':['a','b']",
        ",'conditions':{'no-such-market':{'above':3}}",
        ",'conditions':{'" + KALSHI_ID + "-T3.0':{'over':3}}"}) {
      assertThrows(IllegalArgumentException.class,
          () -> tools().priceEvent(args(kalshiSpec(bad))), bad);
    }
    assertThrows(IllegalArgumentException.class,
        () -> tools().priceEvent(args("{'source':'kalshi'}")));
    assertThrows(IllegalArgumentException.class,
        () -> tools().priceEvent(args("{'source':'predictit','event_id':'x'}")));
  }

  // ─── Fees ──────────────────────────────────────────────────────────────────

  @Test void feesAreReadFromEachVenue() throws Exception {
    assertEquals(0.0175, PredictionMarkets.takerFee(0.07, 0.5), 1e-12);
    PredictionMarkets.LiveEvent k = PredictionMarkets.fetchEvent(venues(), "kalshi", KALSHI_ID);
    assertEquals(0.07, k.feeRates.get(KALSHI_ID + "-T3.0"), 1e-12);
    PredictionMarkets.LiveEvent p =
        PredictionMarkets.fetchEvent(venues(), "polymarket", POLY_ID);
    assertEquals(0.0, p.feeRates.get("m1"), 0.0);

    FakeFetcher charged = venues();
    ObjectNode ev = polymarketEvent();
    ObjectNode m = (ObjectNode) ev.get("markets").get(0);
    m.put("feesEnabled", true);
    m.putObject("feeSchedule").put("exponent", 1).put("rate", 0.03);
    charged.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/" + POLY_ID, ev);
    assertEquals(0.03,
        PredictionMarkets.fetchEvent(charged, "polymarket", POLY_ID).feeRates.get("m1"),
        1e-12);
  }

  @Test void aFeeTypeThatIsNotModelledRaises() {
    FakeFetcher flat = venues();
    ObjectNode series = MAPPER.createObjectNode();
    series.putObject("series").put("fee_type", "flat").put("fee_multiplier", 1);
    flat.byPrefix.put(PredictionMarkets.KALSHI + "/series/KXCPI", series);
    assertThrows(IllegalStateException.class,
        () -> PredictionMarkets.fetchEvent(flat, "kalshi", KALSHI_ID));

    FakeFetcher curved = venues();
    ObjectNode ev = polymarketEvent();
    ObjectNode m = (ObjectNode) ev.get("markets").get(0);
    m.put("feesEnabled", true);
    m.putObject("feeSchedule").put("exponent", 2).put("rate", 0.03);
    curved.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/" + POLY_ID, ev);
    assertThrows(IllegalStateException.class,
        () -> PredictionMarkets.fetchEvent(curved, "polymarket", POLY_ID));

    FakeFetcher silent = venues();
    ObjectNode bare = polymarketEvent();
    ((ObjectNode) bare.get("markets").get(0)).remove("feesEnabled");
    silent.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/" + POLY_ID, bare);
    assertThrows(RuntimeException.class,
        () -> PredictionMarkets.fetchEvent(silent, "polymarket", POLY_ID));
  }

  // ─── Baskets ───────────────────────────────────────────────────────────────

  @Test void theBasketFinderProposesTheCrossVenuePairFirst() throws Exception {
    JsonNode out = call(tools().findBaskets(args("{}")));
    assertEquals(1, out.get("candidates_by_venue").get("kalshi").asInt());
    assertEquals(1, out.get("candidates_by_venue").get("polymarket").asInt());
    assertEquals(1, out.get("baskets_by_recipe").get("cross_venue").asInt());
    JsonNode first = out.get("shown").get(0);
    assertTrue(first.get("basket").asText().startsWith("cross_venue:inflation:"),
        first.get("basket").asText());
    assertEquals(2, first.get("venues").size());
    assertEquals(2, first.get("event_list").size());
    assertThrows(IllegalArgumentException.class,
        () -> tools().findBaskets(args("{'recipe':'same_quantity'}")));
  }

  @Test void aCrossVenuePairLocksNetOfFeesWithNoForecast() throws Exception {
    JsonNode out = call(tools().priceBasket(args("{'events':["
        + kalshiSpec(",'column':'cpi'") + "," + polySpec(",'column':'cpi'")
        + "],'lock':true,'scenario_grid':true,'search':2,'min_yield':0.05}")));
    assertEquals(2, out.get("venues").size());
    assertFalse(out.has("venue_note"));
    JsonNode search = out.get("search");
    assertTrue(search.get("kept").asInt() >= 1, search.toString());
    JsonNode best = search.get("best").get(0);
    double cost = 0.44 + 0.07 * 0.44 * 0.56 + 0.46;
    assertEquals(cost, best.get("cost").asDouble(), 1e-4);
    assertEquals(1 - cost, best.get("worst").asDouble(), 1e-4);
    // floor is the worst case as a return on cost: the yield the lock guarantees.
    assertEquals((1 - cost) / cost, best.get("floor").asDouble(), 1e-4);
    assertEquals(2, best.get("legs").size());
    Set<String> sides = new HashSet<>();
    for (JsonNode leg : best.get("legs")) {
      sides.add(leg.get("source").asText() + ":" + leg.get("side").asText());
    }
    assertEquals(Set.of("kalshi:yes", "polymarket:no"), sides);
  }

  @Test void feesCanEraseTheLock() throws Exception {
    JsonNode out = call(tools().priceBasket(args("{'events':["
        + kalshiSpec(",'column':'cpi','fee_rate':0.5") + ","
        + polySpec(",'column':'cpi','fee_rate':0.5")
        + "],'lock':true,'scenario_grid':true,'search':2}")));
    assertEquals(0, out.get("search").get("kept").asInt());
  }

  @Test void aForecastBasketIsScoredAcrossJointScenarios() throws Exception {
    JsonNode out = call(tools().priceBasket(args("{'events':["
        + kalshiSpec(",'column':'cpi','mean':3.3,'sd':0.1") + ","
        + polySpec(",'column':'cpi','mean':3.3,'sd':0.1")
        + "],'scenarios':[{'CPI':3.3},{'CPI':3.1},{'CPI':2.9}]}")));
    assertEquals(3, out.get("scenarios").asInt());
    assertEquals(3, out.get("all_legs").get("legs").size());
    // Every leg is a Yes above a strike: all pay at 3.3, none at 2.9.
    double cost = out.get("all_legs").get("cost").asDouble();
    assertEquals(-cost, out.get("all_legs").get("worst").asDouble(), 1e-4);
    assertEquals(-1.0, out.get("all_legs").get("floor").asDouble(), 1e-4);
    assertEquals(3 - cost, out.get("all_legs").get("best").asDouble(), 1e-4);
    assertEquals(2.0 / 3, out.get("all_legs").get("p_profit").asDouble(), 1e-4);
  }

  @Test void scenariosComeFromTheCatalogToo() throws Exception {
    MarketTools.SqlRunner history = (q, limit) -> {
      ArrayNode rows = MAPPER.createArrayNode();
      for (double v : new double[]{2.9, 3.1, 3.3, 3.5}) {
        rows.addObject().put("cpi", v);
      }
      return rows;
    };
    JsonNode out = call(tools(venues(), history, 10_000L).priceBasket(args("{'events':["
        + kalshiSpec(",'column':'cpi','mean':3.3,'sd':0.1")
        + "],'scenarios_sql':'select cpi from t'}")));
    assertEquals(4, out.get("scenarios").asInt());
    assertTrue(out.get("scenario_basis").asText().contains("4 rows"));
    assertTrue(out.get("venue_note").asText().contains("other venue"));
  }

  @Test void basketArgumentsAreValidated() {
    String both = kalshiSpec(",'column':'cpi'") + "," + polySpec(",'column':'cpi'");
    for (String bad : new String[]{
        "{'events':[]}",
        "{'events':[" + both + "],'lock':true}",
        "{'events':[" + both + "],'scenario_grid':true}",
        "{'events':[" + both + "],'lock':true,'scenario_grid':true,'scenarios':[{'cpi':3}]}",
        "{'events':[" + both + "],'scenarios':[{'cpi':3}]}",
        "{'events':[" + kalshiSpec(",'colum':'cpi'") + "],'lock':true,'scenario_grid':true}",
        "{'events':[" + kalshiSpec("") + "," + polySpec("")
            + "],'lock':true,'scenario_grid':true}",
        "{'events':[" + kalshiSpec(",'column':'cpi','mean':3.3,'sd':0.1")
            + "],'scenarios':[{'pce':3}]}",
        "{'events':[" + kalshiSpec(",'column':'cpi','mean':3.3,'sd':0.1")
            + "],'scenarios':[{'cpi':3,'p':0.4},{'cpi':3.3,'p':0.4}]}"}) {
      assertThrows(IllegalArgumentException.class,
          () -> tools().priceBasket(args(bad)), bad);
    }
  }

  // ─── Registration ──────────────────────────────────────────────────────────

  @Test void theToolsAreRegisteredAndNameTheForecastingToolsThatExist() {
    Map<String, JsonNode> defs = new LinkedHashMap<>();
    for (JsonNode t : McpServer.toolDefs()) {
      defs.put(t.path("name").asText(), t);
    }
    StringBuilder all = new StringBuilder();
    for (String name : new String[]{"find_market_candidates", "price_market_event",
        "find_market_baskets", "price_market_basket"}) {
      JsonNode t = defs.get(name);
      assertNotNull(t, name + " is not registered");
      String description = t.get("description").asText();
      // Claude Code silently truncates a tool description at 2048 characters.
      assertTrue(description.length() <= 2048, name + " is " + description.length());
      assertTrue(description.contains("Kalshi") && description.contains("Polymarket"), name);
      assertFalse(t.get("inputSchema").get("properties").has("source")
          && !t.get("inputSchema").get("required").toString().contains("source"),
          name + " must not offer a single-venue filter");
      all.append(description).append(t.get("inputSchema"));
    }
    for (String name : new String[]{"forecast_market_event", "market_price_history",
        "scan_market_opportunities"}) {
      JsonNode t = defs.get(name);
      assertNotNull(t, name + " is not registered");
      int length = t.get("description").asText().length();
      assertTrue(length <= 2048, name + " is " + length);
    }
    for (String forecaster : new String[]{"arima_forecast", "garch_forecast",
        "volatility_forecast", "backtest_volatility", "fetch_aligned_series",
        "ols_regression", "flexible_regression", "cross_validate", "scenario_sweep",
        "correlation_matrix"}) {
      assertTrue(defs.containsKey(forecaster), forecaster + " is not a registered tool");
      assertTrue(all.indexOf(forecaster) >= 0,
          "no market tool mentions " + forecaster);
    }
  }

  @Test void theRecipeIsFoundByTheWordsOfTheQuestion() {
    for (String topic : new String[]{"find a random mispriced market event",
        "basket that can lock in a yield", "kalshi polymarket arbitrage"}) {
      assertTrue(RecipeCatalog.find(topic, 5)
          .contains("prediction-market-mispricing-and-arbitrage"), topic);
    }
  }

  /** CPI rising a steady 0.25% a month, to August 2026: a 12-month rate near 3.04%. */
  private static ArrayNode cpiRows() {
    ArrayNode rows = MAPPER.createArrayNode();
    java.time.YearMonth start = java.time.YearMonth.of(2024, 1);
    for (int i = 0; i < 32; i++) {
      java.time.YearMonth ym = start.plusMonths(i);
      ObjectNode r = rows.addObject();
      r.put("year", ym.getYear());
      r.put("period", String.format("M%02d", ym.getMonthValue()));
      r.put("value", 100 * Math.pow(1.0025, i));
    }
    return rows;
  }

  @Test void buildForecastPricesWithTheEnginesOwnForecast() throws Exception {
    List<String> queries = new ArrayList<>();
    MarketTools t = tools(venues(), (q, limit) -> {
      queries.add(q);
      return cpiRows();
    }, 10_000L);
    JsonNode out = call(t.priceEvent(args(kalshiSpec(",'build_forecast':true"))));
    JsonNode built = out.get("forecast_built");
    assertEquals("forecast", built.get("status").asText());
    assertEquals("CUUR0000SA0", built.get("series").asText());
    assertEquals("yoy_pct", built.get("transform").asText());
    assertFalse(built.has("samples"), "samples stay in the engine");
    assertFalse(queries.isEmpty());
    assertFalse(out.get("forecast").isNull());
    assertTrue(out.get("priced_markets").get(0).get("fair").isNumber());
    assertNotNull(out.get("chart_panel"));
  }

  @Test void buildForecastTakesNoOtherForecast() {
    MarketTools t = tools();
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> t.priceEvent(args(kalshiSpec(",'build_forecast':true,'mean':3.0,'sd':0.1"))));
    assertTrue(e.getMessage().contains("build_forecast"), e.getMessage());
  }
}
