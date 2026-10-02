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
import java.util.Iterator;
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

  /** A Kalshi book: resting No bids (a No bid at p is a Yes ask at 1 - p) and Yes bids. */
  private static ObjectNode kalshiBook(String[][] noBids, String[][] yesBids) {
    ObjectNode doc = MAPPER.createObjectNode();
    ObjectNode book = doc.putObject("orderbook_fp");
    ArrayNode no = book.putArray("no_dollars");
    for (String[] level : noBids) {
      no.addArray().add(level[0]).add(level[1]);
    }
    ArrayNode yes = book.putArray("yes_dollars");
    for (String[] level : yesBids) {
      yes.addArray().add(level[0]).add(level[1]);
    }
    return doc;
  }

  private static String bookUrl(String ticker) {
    return PredictionMarkets.KALSHI + "/markets/" + ticker + "/orderbook";
  }

  private static FakeFetcher venues() {
    FakeFetcher f = new FakeFetcher();
    // Books first: a market's URL is a prefix of its book's.
    // T3.0 quotes 0.40 / 0.44: 300 Yes offered at 0.44, 500 at 0.50, 1000 at 0.95.
    f.byPrefix.put(bookUrl(KALSHI_ID + "-T3.0"), kalshiBook(
        new String[][]{{"0.0500", "1000.00"}, {"0.5000", "500.00"}, {"0.5600", "300.00"}},
        new String[][]{{"0.4000", "200.00"}}));
    f.byPrefix.put(bookUrl(KALSHI_ID + "-T3.2"), kalshiBook(
        new String[][]{{"0.7600", "150.00"}}, new String[][]{{"0.2000", "100.00"}}));
    for (JsonNode m : kalshiEvent().get("markets")) {
      ObjectNode one = MAPPER.createObjectNode();
      ObjectNode market = ((ObjectNode) m).deepCopy();
      market.put("event_ticker", KALSHI_ID);
      one.set("market", market);
      f.byPrefix.put(PredictionMarkets.KALSHI + "/markets/" + m.get("ticker").asText(), one);
    }
    ObjectNode polyMarket = ((ObjectNode) polymarketEvent().get("markets").get(0)).deepCopy();
    polyMarket.put("acceptingOrders", true);
    polyMarket.put("clobTokenIds", "[\"yes1\", \"no1\"]");
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/markets/m1", polyMarket);
    ObjectNode polyBook = MAPPER.createObjectNode();
    polyBook.putArray("bids").addObject().put("price", "0.54").put("size", "400");
    polyBook.putArray("asks").addObject().put("price", "0.56").put("size", "250");
    f.byPrefix.put(MarketHistory.CLOB + "/book?token_id=yes1", polyBook);
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

  @Test void aBetweenConditionNeedsItsLowBoundFirst() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> MarketPricing.Condition.parse(mapper.readTree("{\"between\":[3.6,3.5]}")));
    assertTrue(e.getMessage().contains("low <= high"), e.getMessage());
    assertTrue(MarketPricing.Condition.parse(mapper.readTree("{\"between\":[3.0,3.0]}"))
        .holds(3.0));
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

  /** Composes a resolved dashboard, as the report tool does: a layout it rejects fails here. */
  private static String render(JsonNode dashboard) {
    List<DashboardLayout.Panel> panels = new ArrayList<>();
    for (JsonNode p : dashboard.get("panels")) {
      panels.add(McpServer.readPanel(p));
    }
    int columns = dashboard.get("columns").asInt();
    int[] size = DashboardLayout.defaultSize(panels, columns);
    return DashboardLayout.compose(dashboard.get("title").asText(),
        dashboard.get("subtitle").asText(), dashboard.get("footnote").asText(), panels,
        columns, size[0], size[1]).toSvg();
  }

  private static ObjectNode layoutArg(JsonNode out) {
    ObjectNode dash = MAPPER.createObjectNode();
    dash.set(MarketPresentation.LAYOUT, out.get(MarketPresentation.LAYOUT_FIELD));
    return dash;
  }

  @Test void aForecastPricingReturnsItsCardAndTheReportOwesIt() throws Exception {
    MarketTools t = tools();
    MarketPresentation view = new MarketPresentation();
    JsonNode quotes = call(view.event(t.priceEvent(args(kalshiSpec("")))));
    assertFalse(quotes.has(MarketPresentation.LAYOUT_FIELD));
    assertNull(view.gate(false));

    JsonNode out = call(view.event(t.priceEvent(args(kalshiSpec(",'mean':3.0,'sd':0.1")))));
    assertFalse(out.has("chart_panel"));
    String id = "event:kalshi:" + KALSHI_ID;
    assertEquals(id, out.get(MarketPresentation.LAYOUT_FIELD).asText());
    assertTrue(out.get("dashboard_use").asText().contains("{\"layout\": \"" + id + "\"}"));
    assertTrue(out.get("dashboard_panels").size() >= 6, out.get("dashboard_panels").toString());
    assertTrue(view.gate(false).contains(id), view.gate(false));
    assertNull(view.gate(true));

    ObjectNode dash = layoutArg(out);
    assertTrue(view.resolve(dash));
    assertFalse(dash.has(MarketPresentation.LAYOUT));
    assertEquals(out.get("dashboard_panels").size(), dash.get("panels").size());
    // The quote panel: fair value of each market beside the venue's ask.
    JsonNode quote = null;
    for (JsonNode p : dash.get("panels")) {
      if ("line".equals(p.path("chart_type").asText())) {
        quote = p;
      }
    }
    assertNotNull(quote, dash.toString());
    assertEquals(2, quote.get("categories").size());
    assertEquals(out.get("priced_markets").get(0).get("fair").asDouble(),
        quote.get("series").get(0).get("values").get(0).asDouble(), 1e-9);
    assertEquals(0.44, quote.get("series").get(1).get("values").get(0).asDouble(), 1e-9);
    assertEquals(0.24, quote.get("series").get(1).get("values").get(1).asDouble(), 1e-9);
    assertTrue(render(dash).contains("<svg"));

    view.reset();
    assertNull(view.gate(false));
    // A published report does not forget the layout: the same id still resolves.
    assertTrue(view.resolve(layoutArg(out)));
  }

  @Test void aLayoutArgumentIsResolvedOrRefused() throws Exception {
    MarketTools t = tools();
    MarketPresentation view = new MarketPresentation();
    ObjectNode none = MAPPER.createObjectNode();
    none.putArray("panels");
    assertFalse(view.resolve(none));
    ObjectNode unknown = MAPPER.createObjectNode().put(MarketPresentation.LAYOUT, "event:x:y");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> view.resolve(unknown));
    assertTrue(e.getMessage().contains("none has been returned"), e.getMessage());

    JsonNode out = call(view.event(t.priceEvent(args(kalshiSpec(",'mean':3.0,'sd':0.1")))));
    int n = out.get("dashboard_panels").size();
    ObjectNode dash = MAPPER.createObjectNode();
    dash.putArray(MarketPresentation.LAYOUT).add(out.get(MarketPresentation.LAYOUT_FIELD))
        .add(out.get(MarketPresentation.LAYOUT_FIELD));
    dash.put("title", "Two cards");
    dash.putArray("panels").addObject().put("type", "stat").put("label", "Mine")
        .put("value", "1");
    assertTrue(view.resolve(dash));
    // Each layout opens with a heading tile; the caller's panel comes last.
    assertEquals(2 * (n + 1) + 1, dash.get("panels").size());
    assertEquals("1 of 2", dash.get("panels").get(0).get("label").asText());
    assertEquals("2 of 2", dash.get("panels").get(n + 1).get("label").asText());
    assertTrue(dash.get("panels").get(0).get("value").asText().contains("kalshi"),
        dash.get("panels").get(0).toString());
    assertEquals("Mine", dash.get("panels").get(2 * n + 2).get("label").asText());
    assertEquals("Two cards", dash.get("title").asText());
    assertFalse(dash.get("subtitle").asText().contains("Forecast median"));
    assertEquals(4, dash.get("columns").asInt());
    assertTrue(render(dash).contains("<svg"));
    assertThrows(IllegalArgumentException.class,
        () -> view.resolve(MAPPER.createObjectNode().put(MarketPresentation.LAYOUT, 3)));
  }

  @Test void everyPricedEventCarriesFollowUpsItsToolsAccept() throws Exception {
    MarketPresentation view = new MarketPresentation();
    for (String spec : new String[] {kalshiSpec(""), kalshiSpec(",'mean':3.0,'sd':0.1"),
        polySpec(""), polySpec(",'mean':3.0,'sd':0.1")}) {
      JsonNode out = call(view.event(tools().priceEvent(args(spec))));
      assertTrue(out.get("follow_ups").size() >= 1, out.toString());
      assertTrue(out.get("follow_ups").size() <= 5);
      assertTrue(out.get("follow_ups_use").asText().startsWith("You MUST end the answer"));
      for (JsonNode f : out.get("follow_ups")) {
        assertToolAccepts(f);
      }
    }
  }

  /** A follow-up names a registered tool and only arguments its schema defines. */
  static void assertToolAccepts(JsonNode followUp) {
    String tool = followUp.get("tool").asText();
    JsonNode def = null;
    for (JsonNode d : McpServer.toolDefs()) {
      if (tool.equals(d.get("name").asText())) {
        def = d;
      }
    }
    assertNotNull(def, tool);
    assertFalse(followUp.get("question").asText().isEmpty());
    Iterator<String> names = followUp.get("arguments").fieldNames();
    while (names.hasNext()) {
      String name = names.next();
      assertTrue(def.get("inputSchema").get("properties").has(name), tool + "." + name);
    }
    for (JsonNode r : def.get("inputSchema").path("required")) {
      assertTrue(followUp.get("arguments").has(r.asText()), tool + " needs " + r.asText());
    }
  }

  @Test void aLockCarriesItsBasketSheetAndABasketWithNoLockDoesNot() throws Exception {
    MarketPresentation view = new MarketPresentation();
    JsonNode out = call(view.basket(tools().priceBasket(args("{'events':["
        + kalshiSpec(",'column':'cpi'") + "," + polySpec(",'column':'cpi'")
        + "],'lock':true,'scenario_grid':true,'search':2,'min_yield':0.05}"))));
    assertEquals("basket:1", out.get(MarketPresentation.LAYOUT_FIELD).asText());
    ObjectNode dash = layoutArg(out);
    assertTrue(view.resolve(dash));
    List<String> titles = new ArrayList<>();
    for (JsonNode p : dash.get("panels")) {
      titles.add(p.path("title").asText(p.path("label").asText()));
    }
    assertTrue(titles.contains("Profit by settlement value"), titles.toString());
    assertTrue(titles.contains("Rules verdict"), titles.toString());
    assertTrue(render(dash).contains("<svg"));
    for (JsonNode f : out.get("follow_ups")) {
      assertToolAccepts(f);
    }
    assertEquals("compare_settlement_rules", out.get("follow_ups").get(0).get("tool").asText());

    JsonNode none = call(view.basket(tools().priceBasket(args("{'events':["
        + kalshiSpec(",'column':'cpi','fee_rate':0.5") + ","
        + polySpec(",'column':'cpi','fee_rate':0.5")
        + "],'lock':true,'scenario_grid':true,'search':2}"))));
    assertFalse(none.has(MarketPresentation.LAYOUT_FIELD));

    JsonNode scored = call(view.basket(tools().priceBasket(args("{'events':["
        + kalshiSpec(",'column':'cpi','mean':3.3,'sd':0.1") + ","
        + polySpec(",'column':'cpi','mean':3.3,'sd':0.1")
        + "],'scenarios':[{'CPI':3.3},{'CPI':3.1},{'CPI':2.9}]}"))));
    ObjectNode sheet = layoutArg(scored);
    assertTrue(view.resolve(sheet));
    assertTrue(render(sheet).contains("<svg"));
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

  @Test void aCrossVenueLockCarriesItsRulesVerdictAndPayoffCurve() throws Exception {
    JsonNode out = call(tools().priceBasket(args("{'events':["
        + kalshiSpec(",'column':'cpi'") + "," + polySpec(",'column':'cpi'")
        + "],'lock':true,'scenario_grid':true,'search':2,'min_yield':0.05}")));
    // Neither text names a series id or says how revisions are handled.
    assertEquals("unverified", out.get("rules_match").asText(), out.toString());
    JsonNode pair = out.get("rules").get(0);
    assertEquals("kalshi:" + KALSHI_ID, pair.get("a").asText());
    assertEquals("polymarket:" + POLY_ID, pair.get("b").asText());
    assertEquals(0, pair.get("differing").size(), pair.toString());
    Set<String> unknown = new HashSet<>();
    for (JsonNode d : pair.get("unknown")) {
      unknown.add(d.asText());
    }
    assertTrue(unknown.contains("series") && unknown.contains("revision_handling"),
        unknown.toString());
    assertTrue(out.get("next").asText().contains("MUST be reported as not a lock"),
        out.get("next").asText());

    JsonNode best = out.get("search").get("best").get(0);
    JsonNode curve = out.get("payoff_curve");
    assertEquals("search.best[0]", curve.get("of").asText());
    assertEquals("cpi", curve.get("column").asText());
    assertEquals(best.get("cost").asDouble(), curve.get("cost").asDouble(), 1e-4);
    assertEquals(best.get("floor").asDouble(),
        curve.get("floor_profit_per_cost").asDouble(), 1e-4);
    assertTrue(curve.get("curve").size() >= 3, curve.toString());
  }

  @Test void aSearchThatKeepsNothingHasNoPayoffCurve() throws Exception {
    JsonNode out = call(tools().priceBasket(args("{'events':["
        + kalshiSpec(",'column':'cpi','fee_rate':0.5") + ","
        + polySpec(",'column':'cpi','fee_rate':0.5")
        + "],'lock':true,'scenario_grid':true,'search':2}")));
    assertFalse(out.has("payoff_curve"));
    assertTrue(out.get("next").asText().contains("no lock exists"), out.get("next").asText());
  }

  @Test void aCrossVenueBasketIsProposedWithItsRulesVerdict() throws Exception {
    JsonNode first = call(tools().findBaskets(args("{}"))).get("shown").get(0);
    assertEquals("unverified", first.get("rules_match").asText(), first.toString());
    assertEquals(1, first.get("rules").size());
  }

  @Test void anEventIsPricedWithItsStructuralLocksNetOfFees() throws Exception {
    JsonNode out = call(tools().priceEvent(args(kalshiSpec(""))));
    // YES above 3.0 is asked 0.44 and YES above 3.2 is bid 0.20: no ladder pair locks.
    assertEquals(0, out.get("structural_locks").size(), out.get("structural_locks").toString());
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
        "scan_market_opportunities", "backtest_market_forecast",
        "requote_market_opportunity"}) {
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

  @Test void eachForecastAndBasketRecipeIsFoundByAQuestionOfItsOwn() {
    String[][] asked = {
        {"forecast the cpi event on kalshi", "prediction-market-forecast-cpi-pce-food-prices"},
        {"nonfarm payrolls event on a prediction market",
            "prediction-market-forecast-jobs-unemployment-claims"},
        {"gdp growth market on polymarket", "prediction-market-forecast-gdp-and-output"},
        {"30-year mortgage rate market",
            "prediction-market-forecast-mortgage-rate-and-housing"},
        {"highest temperature event on kalshi",
            "prediction-market-forecast-weather-and-climatology"},
        {"will wti crude touch a price at any time, an oil price barrier market",
            "prediction-market-forecast-oil-price-barrier"},
        {"the same event on kalshi and polymarket, a cross venue basket",
            "prediction-market-basket-cross-venue"},
        {"a basket of events in one state, same place basket",
            "prediction-market-basket-same-place"},
        {"linked drivers basket of inflation and fed policy",
            "prediction-market-basket-linked-drivers"},
        {"series run basket of consecutive cpi events", "prediction-market-basket-series-run"},
        {"range basket on a strike ladder", "prediction-market-basket-range"},
        {"calendar basket of two adjacent close dates", "prediction-market-basket-calendar"},
        {"is the mispricing still there, requote the opportunity",
            "prediction-market-capture-a-point-in-time-opportunity"}};
    for (String[] q : asked) {
      assertTrue(RecipeCatalog.find(q[0], 5).contains(q[1]), q[0]);
    }
  }

  /** CPI rising a steady 0.25% a month, to August 2026: a 12-month rate near 3.04%. */
  private static ArrayNode cpiRows() {
    return cpiRows(32);
  }

  private static ArrayNode cpiRows(int months) {
    ArrayNode rows = MAPPER.createArrayNode();
    java.time.YearMonth start = java.time.YearMonth.of(2024, 1);
    for (int i = 0; i < months; i++) {
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
    JsonNode shown = call(new MarketPresentation().event(MAPPER.writeValueAsString(out)));
    MarketPresentation view = new MarketPresentation();
    JsonNode again = call(view.event(MAPPER.writeValueAsString(out)));
    ObjectNode dash = layoutArg(again);
    assertTrue(view.resolve(dash));
    assertEquals(shown.get("dashboard_panels"), again.get("dashboard_panels"));
    // Built by the engine, the forecast brings its history: the card ends with the fan.
    JsonNode fan = dash.get("panels").get(dash.get("panels").size() - 1);
    assertEquals("fan", fan.get("chart_type").asText());
    assertTrue(fan.get("reference_lines").size() >= 1);
    assertTrue(render(dash).contains("class=\"band\""));
  }

  // ─── Tickets and re-quotes ─────────────────────────────────────────────────

  private static JsonNode ticketOf(JsonNode priced, String marketId) {
    for (JsonNode m : priced.get("priced_markets")) {
      if (m.get("market_id").asText().equals(marketId)) {
        return m.get("ticket");
      }
    }
    throw new AssertionError("no market " + marketId);
  }

  private static JsonNode requoteArgs(JsonNode ticket) {
    ObjectNode a = MAPPER.createObjectNode();
    a.set("ticket", ticket);
    return a;
  }

  @Test void aMispricedMarketCarriesATicketAndTheDepthBehindIt() throws Exception {
    JsonNode out = call(tools().priceEvent(
        args(kalshiSpec(",'mean':3.3,'sd':0.1,'min_edge':0.10,'size':400"))));
    assertEquals(2, out.get("books_read").asInt());
    JsonNode m = out.get("priced_markets").get(0);
    assertEquals(KALSHI_ID + "-T3.0", m.get("market_id").asText());
    assertTrue(m.get("annualized_return_simple_365d").asDouble() > 0, m.toString());
    assertTrue(m.get("breakeven_fair").isNumber());
    JsonNode t = m.get("ticket");
    assertEquals("yes", t.get("side").asText());
    // fair 0.9987, fee rate 0.07: the largest tick with 0.9987 - p - 0.07 p (1 - p) >= 0.10.
    assertEquals(0.89, t.get("limit_price").asDouble(), 1e-9, t.toString());
    assertTrue(t.get("edge_at_limit").asDouble() >= 0.10, t.toString());
    assertEquals(NOW.toString(), t.get("quote_time").asText());
    assertEquals(MarketTools.VOID_CLOSE, t.get("void_conditions").get(0).get("type").asText());
    assertEquals(1, t.get("void_conditions").size(), "no built forecast, no release condition");
    JsonNode depth = m.get("depth");
    assertTrue(depth.get("book_read").asBoolean());
    assertEquals(800, depth.get("contracts_at_or_under_limit").asDouble(), 1e-9);
    assertEquals(300 * 0.44 + 500 * 0.50, depth.get("dollars_at_or_under_limit").asDouble(),
        1e-6);
    assertEquals((300 * 0.44 + 100 * 0.50) / 400, depth.get("fill_average_price").asDouble(),
        1e-4);
    assertTrue(depth.get("fills_completely_under_limit").asBoolean());
    assertTrue(out.get("ticket_use").asText().contains("requote_market_opportunity"));
  }

  @Test void aRequoteSaysWhetherTheTicketIsOpenPartlyOpenOrGone() throws Exception {
    FakeFetcher f = venues();
    MarketTools t = tools(f, (q, limit) -> {
      throw new AssertionError("no query expected: " + q);
    }, 10_000L);
    JsonNode ticket = ticketOf(call(t.priceEvent(
        args(kalshiSpec(",'mean':3.3,'sd':0.1,'min_edge':0.10,'size':400")))),
        KALSHI_ID + "-T3.0");

    JsonNode open = call(t.requote(requoteArgs(ticket)));
    assertEquals("open", open.get("status").asText(), open.toString());
    assertEquals(0.44, open.get("current_best_price").asDouble(), 1e-9);
    assertTrue(open.get("release_since_quote").isNull());
    assertEquals(0, open.get("voided_by").size());
    assertEquals(NOW.toString(), open.get("requoted_at").asText());

    f.byPrefix.put(bookUrl(KALSHI_ID + "-T3.0"), kalshiBook(
        new String[][]{{"0.0500", "1000.00"}, {"0.5600", "100.00"}}, new String[][]{}));
    JsonNode partly = call(t.requote(requoteArgs(ticket)));
    assertEquals("partly_open", partly.get("status").asText(), partly.toString());
    // contracts_left is what still rests at or under the limit, of the 400 asked for.
    assertEquals(100, partly.get("contracts_left").asDouble(), 1e-9);
    assertEquals(400, partly.get("ticket_size").asDouble(), 1e-9);

    f.byPrefix.put(bookUrl(KALSHI_ID + "-T3.0"), kalshiBook(
        new String[][]{{"0.0500", "1000.00"}}, new String[][]{}));
    JsonNode gone = call(t.requote(requoteArgs(ticket)));
    assertEquals("gone", gone.get("status").asText(), gone.toString());
    assertEquals(0.95, gone.get("current_best_price").asDouble(), 1e-9);
    assertTrue(gone.get("edge_at_current_best").asDouble() < 0.10);
    assertTrue(gone.get("next").asText().contains("gone"));
  }

  @Test void aTicketIsVoidOnceItsMarketHasClosed() throws Exception {
    FakeFetcher f = venues();
    JsonNode ticket = ticketOf(call(tools(f, (q, limit) -> cpiRows(), 10_000L).priceEvent(
        args(kalshiSpec(",'mean':3.3,'sd':0.1,'min_edge':0.10")))), KALSHI_ID + "-T3.0");
    MarketTools later = new MarketTools(f,
        new PredictionMarkets.ListingCache(f, Duration.ofMinutes(15)), (q, limit) -> cpiRows(),
        () -> Instant.parse("2026-11-12T13:30:00Z"), 10_000L);
    JsonNode out = call(later.requote(requoteArgs(ticket)));
    assertEquals("void", out.get("status").asText(), out.toString());
    assertEquals(MarketTools.VOID_CLOSE, out.get("voided_by").get(0).get("type").asText());
    assertTrue(out.get("next").asText().contains("MUST NOT report it as available"));
  }

  @Test void aTicketOnABuiltForecastIsVoidOnceTheSeriesPrintsAgain() throws Exception {
    FakeFetcher f = venues();
    int[] months = {32};
    MarketTools t = tools(f, (q, limit) -> cpiRows(months[0]), 10_000L);
    JsonNode ticket = ticketOf(call(t.priceEvent(
        args(kalshiSpec(",'build_forecast':true,'min_edge':0.10")))), KALSHI_ID + "-T3.0");
    JsonNode release = ticket.get("void_conditions").get(1);
    assertEquals(MarketTools.VOID_RELEASE, release.get("type").asText(), ticket.toString());
    assertEquals("CUUR0000SA0", release.get("series").asText());
    assertEquals("2026-08", release.get("last_period").asText());

    JsonNode same = call(t.requote(requoteArgs(ticket)));
    assertFalse(same.get("release_since_quote").asBoolean(), same.toString());
    assertEquals("open", same.get("status").asText(), same.toString());

    months[0] = 33;
    JsonNode printed = call(t.requote(requoteArgs(ticket)));
    assertTrue(printed.get("release_since_quote").asBoolean(), printed.toString());
    assertEquals("void", printed.get("status").asText());
    assertEquals("open", printed.get("book_status").asText());
    assertTrue(printed.get("voided_by").get(0).get("why").asText().contains("2026-09"));
  }

  @Test void aForecastEdgeCarriesItsConfidenceAndTheBacktestThatSetIt() throws Exception {
    FakeFetcher f = venues();
    MarketTools.SqlRunner sql = (q, limit) -> cpiRows();
    MarketBacktest backtest = new MarketBacktest(f, sql, () -> NOW);
    MarketTools t = new MarketTools(f,
        new PredictionMarkets.ListingCache(f, Duration.ofMinutes(15)), sql, () -> NOW, 10_000L,
        backtest);

    JsonNode given = call(t.priceEvent(args(kalshiSpec(",'mean':3.3,'sd':0.1"))))
        .get("confidence");
    assertEquals("weak", given.get("tier").asText());
    assertTrue(given.get("reasons").get(0).asText().contains("given by the caller"));

    JsonNode before = call(t.priceEvent(args(kalshiSpec(",'build_forecast':true"))));
    assertEquals("KXCPI", before.get("venue_series").asText());
    JsonNode unrun = before.get("confidence");
    assertEquals("weak", unrun.get("tier").asText(), unrun.toString());
    assertTrue(unrun.get("backtest").isNull());
    assertTrue(unrun.get("must").asText().contains(
        "backtest_market_forecast(source='kalshi', series='KXCPI')"), unrun.toString());

    ObjectNode report = MAPPER.createObjectNode();
    report.put("source", "kalshi").put("series", "KXCPI")
        .put("verdict", "baseline_beats_market").put("verdict_reason", "r")
        .put("events_scored", 9).put("days_before", 7).put("brier_baseline", 0.05)
        .put("brier_price", 0.09).put("brier_difference", -0.04)
        .put("brier_difference_se", 0.01);
    backtest.remember(report);
    JsonNode after = call(t.priceEvent(args(kalshiSpec(",'build_forecast':true"))))
        .get("confidence");
    assertEquals("backtested", after.get("tier").asText(), after.toString());
    assertEquals(9, after.get("backtest").get("events_scored").asInt());
    assertEquals(NOW.toString(), after.get("backtest").get("run_at").asText());

    assertFalse(call(t.priceEvent(args(kalshiSpec("")))).has("confidence"),
        "quotes alone carry no forecast to grade");
  }

  @Test void aRequoteNeedsATicket() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> tools().requote(args("{}")));
    assertTrue(e.getMessage().contains("ticket is required"), e.getMessage());
  }

  @Test void buildForecastTakesNoOtherForecast() {
    MarketTools t = tools();
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> t.priceEvent(args(kalshiSpec(",'build_forecast':true,'mean':3.0,'sd':0.1"))));
    assertTrue(e.getMessage().contains("build_forecast"), e.getMessage());
  }
}
