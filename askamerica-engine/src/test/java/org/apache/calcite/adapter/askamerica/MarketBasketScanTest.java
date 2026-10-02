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
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The basket scan against canned venue responses: no network, no catalog.
 *
 * <p>The fixture is October CPI on both venues. Kalshi asks 0.44 for "above 3.0"; Polymarket
 * bids whatever the test sets for its market labelled "Above 3.0%". At a 0.60 bid, YES on
 * Kalshi and NO on Polymarket cost 0.84 before fees and pay 1 at every outcome.
 */
@Tag("unit")
class MarketBasketScanTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-10-02T00:00:00Z");
  private static final String KALSHI_ID = "KXCPI-26OCT";
  private static final String POLY_ID = "9001";

  /** Answers a URL from the first registered prefix it starts with. */
  private static final class FakeFetcher implements PredictionMarkets.Fetcher {
    final Map<String, JsonNode> byPrefix = new LinkedHashMap<>();
    int eventReads;
    int sqlReads;

    @Override public JsonNode get(String url) throws IOException {
      if (url.startsWith(PredictionMarkets.KALSHI + "/events/")
          || url.startsWith(PredictionMarkets.POLYMARKET + "/events/" + POLY_ID)) {
        eventReads++;
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
    markets.add(kalshiMarket(KALSHI_ID + "-T2.8", 2.8, "0.7000", "0.7400"));
    markets.add(kalshiMarket(KALSHI_ID + "-T3.0", 3.0, "0.4000", "0.4400"));
    markets.add(kalshiMarket(KALSHI_ID + "-T3.2", 3.2, "0.2000", "0.2400"));
    return ev;
  }

  private static void polymarketMarket(ArrayNode markets, String id, String label, double bid,
      double ask) {
    ObjectNode m = markets.addObject();
    m.put("id", id);
    m.put("question", "US CPI inflation in October: " + (label == null ? id : label) + "?");
    if (label != null) {
      m.put("groupItemTitle", label);
    }
    m.put("active", true);
    m.put("closed", false);
    m.put("outcomes", "[\"Yes\",\"No\"]");
    m.put("outcomePrices", "[\"" + bid + "\",\"" + (1 - bid) + "\"]");
    m.put("bestBid", bid);
    m.put("bestAsk", ask);
    m.put("volumeNum", 20000);
    m.put("volume24hr", 3000);
    m.put("endDate", "2026-11-12T12:00:00Z");
    m.put("feesEnabled", false);
    m.put("description", "Resolves on BLS CPI-U year over year, one decimal.");
  }

  private static ObjectNode polymarketEvent() {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("id", POLY_ID);
    ev.put("title", "US CPI inflation October 2026");
    ev.put("slug", "us-cpi-october-2026");
    ev.putArray("tags").addObject().put("label", "Economy");
    ev.putArray("markets");
    return ev;
  }

  /** One Polymarket market labelled "Above 3.0%", bid as given. */
  private static ObjectNode polymarketAbove(double bid) {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "m1", "Above 3.0%", bid, bid + 0.02);
    return ev;
  }

  private static FakeFetcher venues(ObjectNode polymarket) {
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
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/" + POLY_ID, polymarket);
    ObjectNode pList = MAPPER.createObjectNode();
    pList.putArray("events").add(polymarket);
    pList.put("next_cursor", "");
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/keyset?", pList);
    return f;
  }

  /** Monthly index rows from 2024-01 to 2026-08, the latest print at NOW: 3.0% a year. */
  private static ArrayNode cpiRows() {
    ArrayNode rows = MAPPER.createArrayNode();
    YearMonth start = YearMonth.of(2024, 1);
    for (int i = 0; i < 32; i++) {
      YearMonth ym = start.plusMonths(i);
      ObjectNode r = rows.addObject();
      r.put("year", ym.getYear());
      r.put("period", String.format("M%02d", ym.getMonthValue()));
      r.put("value", 100 * Math.pow(1.0025, i));
    }
    return rows;
  }

  private static MarketTools.SqlRunner sql(FakeFetcher f) {
    return (q, limit) -> {
      f.sqlReads++;
      return cpiRows();
    };
  }

  private static MarketBasketScan scan(FakeFetcher f, long budgetMillis) {
    return new MarketBasketScan(f,
        new PredictionMarkets.ListingCache(f, Duration.ofMinutes(15)), sql(f), () -> NOW,
        10_000L, budgetMillis, Duration.ofMinutes(15));
  }

  private static JsonNode args(String json) throws Exception {
    return MAPPER.readTree(json.replace('\'', '"'));
  }

  private static JsonNode run(ObjectNode polymarket, String json) throws Exception {
    return MAPPER.readTree(scan(venues(polymarket), 60_000L).scan(args(json)));
  }

  @Test void aCrossVenueLockHasALegOnEachVenueAndItsFloorIsNetOfFees() throws Exception {
    JsonNode out = run(polymarketAbove(0.60), "{}");

    assertEquals("complete", out.get("status").asText());
    JsonNode funnel = out.get("funnel");
    assertEquals(2, funnel.get("events_read").asInt());
    assertEquals(1, funnel.get("cross_venue_pairs").asInt());
    assertEquals(1, funnel.get("cross_venue_pairs_priced").asInt());
    assertEquals(1, funnel.get("cross_venue_pairs_with_a_lock").asInt());
    assertEquals(1, funnel.get("events_with_conditions_read_from_labels").asInt());

    JsonNode best = out.get("baskets").get(0);
    assertEquals("cross_venue", best.get("type").asText());
    assertEquals(2, best.get("venues").size());
    assertEquals(2, best.get("events").size());
    // YES above 3.0 on Kalshi at 0.44 plus its fee 0.07 x 0.44 x 0.56, NO on Polymarket at
    // 0.40 with fees off: pays exactly 1 at every outcome.
    double cost = 0.44 + 0.07 * 0.44 * 0.56 + 0.40;
    assertEquals(cost, best.get("cost").asDouble(), 1e-3);
    assertEquals(1 - cost, best.get("floor_profit").asDouble(), 1e-3);
    assertEquals((1 - cost) / cost, best.get("floor").asDouble(), 1e-3);
    assertEquals(2, best.get("legs").size());
    for (JsonNode leg : best.get("legs")) {
      assertEquals(3.0, leg.get("condition").get("above").asDouble(), 1e-12);
      assertEquals("kalshi".equals(leg.get("source").asText()) ? "yes" : "no",
          leg.get("side").asText());
    }
    assertNotNull(best.get("rules_match"));
    assertFalse("differ".equals(best.get("rules_match").asText())
        && best.get("rules_differing").size() == 0);
    for (JsonNode b : out.get("baskets")) {
      assertEquals(2, b.get("venues").size(), "a single-venue subset is not a cross-venue lock");
    }
    assertTrue(out.get("next").asText().contains("rules_match"));
  }

  /** Kalshi's book of one market: resting Yes bids and No bids, as price, size pairs. */
  private static void kalshiBook(FakeFetcher f, String ticker, double[] yes, double[] no) {
    ObjectNode book = MAPPER.createObjectNode();
    ObjectNode fp = book.putObject("orderbook_fp");
    ArrayNode y = fp.putArray("yes_dollars");
    for (int i = 0; i < yes.length; i += 2) {
      y.addArray().add(String.valueOf(yes[i])).add(String.valueOf(yes[i + 1]));
    }
    ArrayNode n = fp.putArray("no_dollars");
    for (int i = 0; i < no.length; i += 2) {
      n.addArray().add(String.valueOf(no[i])).add(String.valueOf(no[i + 1]));
    }
    f.byPrefix.put(PredictionMarkets.KALSHI + "/markets/" + ticker + "/orderbook", book);
  }

  /** Polymarket's market and the book of its Yes token: bids as price, size pairs. */
  private static void polymarketBook(FakeFetcher f, String id, boolean accepting,
      double... bids) {
    ObjectNode m = MAPPER.createObjectNode();
    m.put("question", "market " + id);
    m.put("closed", false);
    m.put("acceptingOrders", accepting);
    m.put("outcomes", "[\"Yes\",\"No\"]");
    m.put("clobTokenIds", "[\"yes-" + id + "\",\"no-" + id + "\"]");
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/markets/" + id, m);
    ObjectNode book = MAPPER.createObjectNode();
    ArrayNode b = book.putArray("bids");
    for (int i = 0; i < bids.length; i += 2) {
      b.addObject().put("price", String.valueOf(bids[i]))
          .put("size", String.valueOf(bids[i + 1]));
    }
    book.putArray("asks");
    f.byPrefix.put(MarketHistory.CLOB + "/book?token_id=yes-" + id, book);
  }

  private static JsonNode sized(FakeFetcher f) throws Exception {
    return MAPPER.readTree(scan(f, 60_000L).scan(args("{}"))).get("baskets").get(0);
  }

  @Test void sizeWalksTheBooksUntilASetCostsWhatItPays() throws Exception {
    FakeFetcher f = venues(polymarketAbove(0.60));
    // Yes asks on Kalshi are its No bids: 0.44 x 30, then 0.50 x 100.
    kalshiBook(f, KALSHI_ID + "-T3.0", new double[] {0.40, 500},
        new double[] {0.56, 30, 0.50, 100});
    // No on Polymarket is bought from its Yes bids: 0.40 x 10, 0.45 x 50, then 0.60.
    polymarketBook(f, "m1", true, 0.60, 10, 0.55, 50, 0.40, 1000);
    JsonNode best = sized(f);

    double first = 0.44 + 0.07 * 0.44 * 0.56 + 0.40;
    double second = 0.44 + 0.07 * 0.44 * 0.56 + 0.45;
    double third = 0.50 + 0.07 * 0.50 * 0.50 + 0.45;
    double capital = 10 * first + 20 * second + 30 * third;
    JsonNode size = best.get("size");
    assertEquals(first, size.get("first_set_cost").asDouble(), 1e-5);
    assertEquals(10, size.get("sets_at_best_price").asDouble(), 1e-9);
    assertEquals("m1", size.get("binding_leg").asText());
    assertEquals(60, size.get("sets_with_a_positive_floor").asDouble(), 1e-9);
    assertEquals(capital, size.get("capital").asDouble(), 0.005);
    assertEquals(60 - capital, size.get("floor_profit").asDouble(), 0.005);
    assertEquals((60 - capital) / capital, size.get("floor").asDouble(), 1e-4);
    assertEquals((60 - capital) / capital * 365 / 41.5625,
        size.get("annualized_floor_simple_365d").asDouble(), 1e-3);
    assertTrue(size.get("annualized_floor_simple_365d").asDouble()
        < best.get("annualized_floor_simple_365d").asDouble());
    // The fourth level pairs 0.50 on Kalshi with 0.60 on Polymarket: over 1 after the fee.
    assertTrue(size.get("stops_because").asText().startsWith("the next set costs 1.1175"),
        size.get("stops_because").asText());
    assertEquals(NOW.toString(), size.get("books_read_at").asText());
    // Cash out at purchase, cash back at settlement: 60 sets each paying at least 1.
    JsonNode flows = size.get("cashflows");
    assertEquals(capital, flows.get("paid_at_purchase").asDouble(), 0.005);
    assertEquals(60, flows.get("received_at_settlement_at_least").asDouble(), 1e-9);
    assertEquals("2026-11-12T13:30:00Z", flows.get("last_event_closes").asText());
  }

  @Test void theFloorIsAnnualizedOverTheDaysToTheLastClose() throws Exception {
    JsonNode best = run(polymarketAbove(0.60), "{}").get("baskets").get(0);

    // Kalshi closes 2026-11-12T13:30Z, Polymarket 12:00Z; the clock reads 2026-10-02T00:00Z.
    assertEquals(41.5625, best.get("days_to_settlement").asDouble(), 1e-3);
    assertEquals(best.get("floor").asDouble() * 365 / 41.5625,
        best.get("annualized_floor_simple_365d").asDouble(), 1e-3);
    assertNull(best.get("annualized_floor_note"));
  }

  @Test void aBasketClosingWithinADayIsNotAnnualized() throws Exception {
    FakeFetcher f = venues(polymarketAbove(0.60));
    MarketBasketScan soon = new MarketBasketScan(f,
        new PredictionMarkets.ListingCache(f, Duration.ofMinutes(15)), sql(f),
        () -> Instant.parse("2026-11-12T00:00:00Z"), 10_000L, 60_000L, Duration.ofMinutes(15));
    JsonNode best = MAPPER.readTree(soon.scan(args("{'min_days': 0}"))).get("baskets").get(0);

    assertNull(best.get("annualized_floor_simple_365d"));
    assertTrue(best.get("annualized_floor_note").asText().contains("not annualized"));
  }

  @Test void sizeStopsWhereALegsBookRunsOut() throws Exception {
    FakeFetcher f = venues(polymarketAbove(0.60));
    kalshiBook(f, KALSHI_ID + "-T3.0", new double[] {}, new double[] {0.56, 30});
    polymarketBook(f, "m1", true, 0.60, 100);
    JsonNode size = sized(f).get("size");

    assertEquals(30, size.get("sets_with_a_positive_floor").asDouble(), 1e-9);
    assertEquals(KALSHI_ID + "-T3.0", size.get("binding_leg").asText());
    assertEquals("the book of leg " + KALSHI_ID + "-T3.0 has no more orders",
        size.get("stops_because").asText());
  }

  @Test void booksThatMovedPastTheQuoteFillNoSet() throws Exception {
    FakeFetcher f = venues(polymarketAbove(0.60));
    // The Yes ask the listing gave at 0.44 now stands at 0.70.
    kalshiBook(f, KALSHI_ID + "-T3.0", new double[] {}, new double[] {0.30, 30});
    polymarketBook(f, "m1", true, 0.60, 100);
    JsonNode size = sized(f).get("size");

    assertEquals(0, size.get("sets_with_a_positive_floor").asDouble(), 1e-9);
    assertEquals(0, size.get("capital").asDouble(), 1e-9);
    assertNull(size.get("floor"));
    assertTrue(size.get("note").asText().contains("moved"), size.get("note").asText());
  }

  @Test void aBookThatCannotBeReadIsSaidNotSized() throws Exception {
    JsonNode best = run(polymarketAbove(0.60), "{}").get("baskets").get(0);

    assertTrue(best.get("size").isNull());
    assertTrue(best.get("size_note").asText().contains("was not read"),
        best.get("size_note").asText());
  }

  @Test void aLegNotTakingOrdersIsSaidNotSized() throws Exception {
    FakeFetcher f = venues(polymarketAbove(0.60));
    kalshiBook(f, KALSHI_ID + "-T3.0", new double[] {}, new double[] {0.56, 30});
    polymarketBook(f, "m1", false, 0.60, 100);
    JsonNode best = sized(f);

    assertTrue(best.get("size").isNull());
    assertEquals("leg m1 is not taking orders: not_accepting_orders",
        best.get("size_note").asText());
  }

  @Test void anExclusiveSetIsSizedAgainstItsWholePayout() throws Exception {
    ObjectNode ev = polymarketEvent();
    ev.put("negRisk", true);
    ArrayNode markets = (ArrayNode) ev.get("markets");
    polymarketMarket(markets, "m1", "Rise", 0.50, 0.52);
    polymarketMarket(markets, "m2", "Fall", 0.40, 0.42);
    polymarketMarket(markets, "m3", "No change", 0.20, 0.22);
    FakeFetcher f = venues(ev);
    polymarketBook(f, "m1", true, 0.50, 40);
    polymarketBook(f, "m2", true, 0.40, 25, 0.25, 500);
    polymarketBook(f, "m3", true, 0.20, 60);
    JsonNode size = sized(f).get("size");

    // 25 sets at 1.90 against a payout of 2; the next would cost 0.50 + 0.75 + 0.80.
    assertEquals(25, size.get("sets_with_a_positive_floor").asDouble(), 1e-9);
    assertEquals("m2", size.get("binding_leg").asText());
    assertEquals(47.5, size.get("capital").asDouble(), 1e-6);
    assertEquals(2.5, size.get("floor_profit").asDouble(), 1e-6);
    assertTrue(size.get("stops_because").asText().endsWith("pays 2"),
        size.get("stops_because").asText());
  }

  @Test void aLockNamesTheSeriesBothEventsSettleOn() throws Exception {
    JsonNode out = run(polymarketAbove(0.60), "{}");

    assertEquals(1, out.get("baskets").size(), "one basket for a pair, its best floor");
    JsonNode best = out.get("baskets").get(0);
    assertEquals("verified", best.get("same_quantity").asText());
    assertEquals("CUUR0000SA0 yoy_pct", best.get("settles_on").asText());
    assertEquals(0, out.get("unverified").size());
  }

  @Test void coreAgainstHeadlineInflationIsNotPricedHoweverWideTheGap() throws Exception {
    ObjectNode core = polymarketAbove(0.60);
    core.put("title", "US core CPI inflation October 2026");

    JsonNode out = run(core, "{}");

    assertEquals("complete", out.get("status").asText());
    assertEquals(0, out.get("baskets").size());
    assertEquals(0, out.get("unverified").size());
    assertEquals(0, out.get("funnel").get("cross_venue_pairs_priced").asInt());
    assertEquals(0, out.get("funnel").get("cross_venue_pairs_with_a_lock").asInt());
    String why = out.get("not_priced").get(0).get("why").asText();
    assertTrue(why.contains("different series"), why);
    assertTrue(why.contains("CUUR0000SA0L1E") && why.contains("CUUR0000SA0 "), why);
  }

  @Test void differentSeriesAreNotPricedEvenWhenOneTransformIsUnread() throws Exception {
    ObjectNode core = polymarketAbove(0.60);
    core.put("title", "US core CPI October 2026");
    for (JsonNode m : core.get("markets")) {
      ((ObjectNode) m).put("question", "US core CPI in October: Above 3.0%?");
      ((ObjectNode) m).put("description", "Resolves on the BLS release.");
    }

    JsonNode out = run(core, "{}");

    assertEquals(0, out.get("baskets").size());
    assertEquals(0, out.get("unverified").size());
    String why = out.get("not_priced").get(0).get("why").asText();
    assertTrue(why.contains("different series: CUUR0000SA0 and "), why);
  }

  @Test void aPairWithOneSeriesUnresolvedIsListedApartAndIsNotALock() throws Exception {
    ObjectNode shelter = polymarketAbove(0.60);
    shelter.put("title", "US shelter CPI inflation October 2026");

    JsonNode out = run(shelter, "{}");

    assertEquals(0, out.get("baskets").size());
    assertEquals(0, out.get("funnel").get("locks_found").asInt());
    assertEquals(0, out.get("funnel").get("cross_venue_pairs_with_a_lock").asInt());
    assertEquals(1, out.get("funnel").get("cross_venue_pairs_unverified_with_a_gap").asInt());
    JsonNode u = out.get("unverified").get(0);
    assertEquals("unverified", u.get("same_quantity").asText());
    assertTrue(u.get("same_quantity_reason").asText().contains("polymarket event"),
        u.get("same_quantity_reason").asText());
    assertNull(u.get("settles_on"));
    assertTrue(out.get("next").asText().contains("MUST NOT report an unverified entry"),
        out.get("next").asText());
  }

  @Test void quotesThatLeaveNoGapGiveNoBasket() throws Exception {
    // Polymarket bids 0.42: NO costs 0.58, and with Kalshi YES at 0.44 the pair costs over 1.
    JsonNode out = run(polymarketAbove(0.42), "{}");

    assertEquals("complete", out.get("status").asText());
    assertEquals(1, out.get("funnel").get("cross_venue_pairs_priced").asInt());
    assertEquals(0, out.get("funnel").get("locks_found").asInt());
    assertEquals(0, out.get("baskets").size());
    assertTrue(out.get("next").asText().startsWith("No basket locks a profit"));
  }

  @Test void minFloorLeavesOutASmallerLock() throws Exception {
    JsonNode out = run(polymarketAbove(0.60), "{'min_floor':0.5}");

    assertEquals(0, out.get("baskets").size());
    assertEquals(0.5, out.get("min_floor").asDouble(), 1e-9);
  }

  @Test void aPairWhoseLabelsStateNoNumberIsNotPricedAndSaysWhy() throws Exception {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "m1", "No change", 0.60, 0.62);
    JsonNode out = run(ev, "{}");

    assertEquals(1, out.get("funnel").get("cross_venue_pairs").asInt());
    assertEquals(0, out.get("funnel").get("cross_venue_pairs_priced").asInt());
    JsonNode not = out.get("not_priced").get(0);
    assertTrue(not.get("why").asText().contains("states a number"), not.toString());
    assertTrue(not.get("why").asText().contains("No change"), not.toString());
    assertEquals(KALSHI_ID, not.get("examples").get(0).asText().split(" and ")[0].split(":")[1]);
  }

  @Test void strikesInDifferentUnitsAreNotPriced() throws Exception {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "m1", "Above 300", 0.60, 0.62);
    JsonNode out = run(ev, "{}");

    assertEquals(0, out.get("funnel").get("cross_venue_pairs_priced").asInt());
    assertTrue(out.get("not_priced").get(0).get("why").asText().contains("do not overlap"));
  }

  @Test void anExclusiveEventLocksWhenItsBidsSumOverOne() throws Exception {
    ObjectNode ev = polymarketEvent();
    ev.put("negRisk", true);
    ArrayNode markets = (ArrayNode) ev.get("markets");
    polymarketMarket(markets, "m1", "Rise", 0.50, 0.52);
    polymarketMarket(markets, "m2", "Fall", 0.40, 0.42);
    polymarketMarket(markets, "m3", "No change", 0.20, 0.22);
    JsonNode out = run(ev, "{}");

    assertEquals(1, out.get("funnel").get("events_the_venue_states_exclusive").asInt());
    assertEquals(1, out.get("funnel").get("events_with_a_lock_inside").asInt());
    JsonNode best = out.get("baskets").get(0);
    assertEquals("exclusive_set", best.get("type").asText());
    assertEquals(3, best.get("legs").size());
    for (JsonNode leg : best.get("legs")) {
      assertEquals("no", leg.get("side").asText());
      assertEquals("polymarket", leg.get("source").asText());
    }
    // NO on three markets costs 0.50 + 0.60 + 0.80 and pays at least 2.
    assertEquals(1.90, best.get("cost").asDouble(), 1e-6);
    assertEquals(0.10, best.get("floor_profit").asDouble(), 1e-6);
    assertEquals(1.10, best.get("yes_bids_sum").asDouble(), 1e-6);
  }

  @Test void anExclusiveEventWithBidsUnderOneIsListedAsClosestNotAsALock() throws Exception {
    ObjectNode ev = polymarketEvent();
    ev.put("negRisk", true);
    ArrayNode markets = (ArrayNode) ev.get("markets");
    polymarketMarket(markets, "m1", "Rise", 0.50, 0.52);
    polymarketMarket(markets, "m2", "Fall", 0.30, 0.32);
    JsonNode out = run(ev, "{}");

    assertEquals(0, out.get("baskets").size());
    JsonNode near = out.get("closest").get(0);
    assertEquals(POLY_ID, near.get("event").get("event_id").asText());
    assertEquals(0.80, near.get("yes_bids_sum").asDouble(), 1e-6);
    assertTrue(near.get("floor_profit").asDouble() < 0);
  }

  @Test void anEventTheVenueDoesNotStateExclusiveClaimsNoExclusiveLock() throws Exception {
    ObjectNode ev = polymarketEvent();
    ArrayNode markets = (ArrayNode) ev.get("markets");
    polymarketMarket(markets, "m1", "Rise", 0.60, 0.62);
    polymarketMarket(markets, "m2", "Fall", 0.60, 0.62);
    JsonNode out = run(ev, "{}");

    assertEquals(0, out.get("funnel").get("events_the_venue_states_exclusive").asInt());
    assertEquals(0, out.get("baskets").size());
    assertEquals(0, out.get("closest").size());
  }

  @Test void aScanOutOfTimeResumesWithoutReadingAnEventTwice() throws Exception {
    FakeFetcher f = venues(polymarketAbove(0.60));
    MarketBasketScan s = scan(f, -1L);

    JsonNode first = MAPPER.readTree(s.scan(args("{}")));
    assertEquals("scanning", first.get("status").asText());
    assertEquals(1, first.get("funnel").get("events_read").asInt());
    assertTrue(first.get("next").asText().contains("call scan_market_baskets again"));
    assertEquals(0, first.get("not_priced").size(), "an unread pair is not a pair left out");
    int reads = f.eventReads;

    JsonNode second = MAPPER.readTree(s.scan(args("{}")));
    assertEquals("complete", second.get("status").asText());
    assertEquals(2, second.get("funnel").get("events_read").asInt());
    assertEquals(reads + 1, f.eventReads);
    assertEquals(1, second.get("baskets").size() > 0 ? 1 : 0);
  }

  @Test void argumentsAreValidated() {
    MarketBasketScan s = scan(venues(polymarketAbove(0.60)), 60_000L);

    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> s.scan(args("{'min_edge':0.1}"))).getMessage().contains("unknown key 'min_edge'"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> s.scan(args("{'driver':'nonsense'}"))).getMessage().contains("is not a driver"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> s.scan(args("{'search':7}"))).getMessage().contains("search must be 2 to 4"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> s.scan(args("{'min_floor':-0.1}"))).getMessage().contains("min_floor"));
  }

  @Test void aLabelIsReadOnlyInTheFormsItStatesANumber() {
    MarketPricing.Condition above = MarketPricing.Condition.ofLabel("Above 4.5%");
    assertEquals("above", above.kind);
    assertEquals(4.5, above.low, 1e-12);
    assertEquals("below", MarketPricing.Condition.ofLabel("<0.5%").kind);
    assertEquals("at_least", MarketPricing.Condition.ofLabel("≥3.0%").kind);
    MarketPricing.Condition atMost = MarketPricing.Condition.ofLabel("≤0.0%");
    assertEquals("at_most", atMost.kind);
    assertEquals(0.0, atMost.low, 1e-12);
    MarketPricing.Condition range = MarketPricing.Condition.ofLabel("0.5–1.0%");
    assertEquals("between", range.kind);
    assertEquals(0.5, range.low, 1e-12);
    assertEquals(1.0, range.high, 1e-12);
    MarketPricing.Condition exact = MarketPricing.Condition.ofLabel("0.3%");
    assertEquals("between", exact.kind);
    assertEquals(0.3, exact.low, 1e-12);
    assertEquals(0.3, exact.high, 1e-12);
    assertEquals(-0.2, MarketPricing.Condition.ofLabel("Below −0.2%").low, 1e-12);
    assertEquals(250000, MarketPricing.Condition.ofLabel("Over 250k").low, 1e-6);
    assertEquals(1500, MarketPricing.Condition.ofLabel("$1,000 to $1,500").high, 1e-6);

    assertNull(MarketPricing.Condition.ofLabel("25 bps decrease"));
    assertNull(MarketPricing.Condition.ofLabel("No change"));
    assertNull(MarketPricing.Condition.ofLabel("250k+"));
    assertNull(MarketPricing.Condition.ofLabel("100k–200m"));
    assertNull(MarketPricing.Condition.ofLabel("3.0–2.0%"));
    assertNull(MarketPricing.Condition.ofLabel(null));
  }

  @Test void aValueTwoLabelledRangesBothClaimWinsForNeitherSide() throws Exception {
    ObjectNode ev = polymarketEvent();
    ArrayNode markets = (ArrayNode) ev.get("markets");
    polymarketMarket(markets, "m1", "2.5–3.0%", 0.30, 0.32);
    polymarketMarket(markets, "m2", "3.0–3.5%", 0.30, 0.32);
    polymarketMarket(markets, "m3", "Above 3.5%", 0.30, 0.32);
    PredictionMarkets.Event event =
        PredictionMarkets.fetchEvent(venues(ev), "polymarket", POLY_ID).event;

    Map<String, MarketPricing.Condition> read = MarketPricing.labelConditions(event);
    assertEquals(3, read.size());
    MarketPricing.Condition lower = read.get("m1");
    MarketPricing.Condition upper = read.get("m2");
    assertTrue(lower.holds(3.0) && upper.holds(3.0));
    assertFalse(lower.wins("yes", 3.0));
    assertFalse(lower.wins("no", 3.0));
    assertFalse(upper.wins("yes", 3.0));
    assertFalse(upper.wins("no", 3.0));
    // Away from the shared end the range is an ordinary condition.
    assertTrue(lower.wins("yes", 2.7));
    assertTrue(lower.wins("no", 3.2));
    assertTrue(lower.wins("yes", 2.5), "an end no other range claims is not in doubt");
    assertTrue(upper.wins("yes", 3.5), "Above 3.5% is not a range and shares no end");
    assertTrue(read.get("m3").wins("no", 3.5));
  }

  @Test void aFinishedScanCarriesItsBoardAndForecastsTheTopBasketFirst() throws Exception {
    String raw = scan(venues(polymarketAbove(0.60)), 60_000L).scan(args("{}"));
    JsonNode out = MAPPER.readTree(new MarketPresentation().basketScan(raw));

    assertTrue(out.get("dashboard_layout").asText().startsWith("basketscan:"));
    assertEquals(6, out.get("dashboard_panels").size());
    JsonNode followUps = out.get("follow_ups");
    assertEquals("price_market_event", followUps.get(0).get("tool").asText());
    assertTrue(followUps.get(0).get("arguments").get("build_forecast").asBoolean());
    assertEquals("price_market_event", followUps.get(1).get("tool").asText());
    boolean depth = false;
    for (JsonNode f : followUps) {
      depth |= "market_price_history".equals(f.get("tool").asText());
    }
    assertTrue(depth);
  }

  @Test void anUnfinishedScanCarriesNoBoard() throws Exception {
    String raw = scan(venues(polymarketAbove(0.60)), -1L).scan(args("{}"));

    assertEquals(raw, new MarketPresentation().basketScan(raw));
  }

  /**
   * Polymarket strikes either side of Kalshi's: no lock, and baskets that lose in between.
   * Above 3.2 is quoted at {@code bid} and {@code ask}; Above 3.5 at 0.10 and 0.12.
   */
  private static ObjectNode polymarketWide(double bid, double ask) {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "lo", "Above 2.9%", 0.72, 0.74);
    polymarketMarket((ArrayNode) ev.get("markets"), "mid", "Above 3.2%", bid, ask);
    polymarketMarket((ArrayNode) ev.get("markets"), "hi", "Above 3.5%", 0.10, 0.12);
    return ev;
  }

  /** Polymarket's quotes put 0.20 - 0.11 = 0.09 on CPI above 3.2 and at or under 3.5. */
  private static ObjectNode polymarketWide() {
    return polymarketWide(0.18, 0.22);
  }

  @Test void aPairWithNoLockYieldsANearLockThatLosesOnlyInsideABand() throws Exception {
    FakeFetcher f = venues(polymarketWide());
    JsonNode out = MAPPER.readTree(scan(f, 60_000L).scan(args("{'search': 2}")));

    assertEquals("complete", out.get("status").asText());
    assertEquals(0, out.get("baskets").size());
    assertEquals(1, out.get("funnel").get("near_locks_found").asInt());
    assertEquals(1, out.get("funnel").get("cross_venue_pairs_forecast").asInt());
    assertTrue(f.sqlReads > 0);
    JsonNode near = out.get("near_locks").get(0);
    assertEquals("near_lock", near.get("type").asText());
    assertEquals(2, near.get("venues").size());
    // NO above 3.2 on Kalshi at 0.80 plus its fee, YES above 3.5 on Polymarket at 0.12: one
    // of them wins unless CPI lands above 3.2 and at or under 3.5, where both lose. NO above
    // 3.0 at 0.60 is cheaper but the forecast puts it at 1: further than 0.20 from its quote.
    double cost = 0.80 + 0.07 * 0.80 * 0.20 + 0.12;
    assertEquals(cost, near.get("cost").asDouble(), 1e-4);
    assertEquals("no", near.get("legs").get(0).get("side").asText());
    assertEquals(KALSHI_ID + "-T3.2", near.get("legs").get(0).get("market_id").asText());
    assertEquals("hi", near.get("legs").get(1).get("market_id").asText());
    assertEquals(1, near.get("legs").get(0).get("forecast_p_win").asDouble(), 1e-12);
    assertEquals(0, near.get("legs").get(1).get("forecast_p_win").asDouble(), 1e-12);
    assertEquals(0.20, near.get("max_quote_gap").asDouble(), 1e-9);
    // The forecast puts the Kalshi leg at 1 where its price says 0.80.
    assertEquals(1.25, near.get("max_quote_ratio").asDouble(), 1e-9);
    assertEquals(-cost, near.get("worst_profit").asDouble(), 1e-4);
    assertEquals(1 - cost, near.get("best_profit").asDouble(), 1e-4);
    JsonNode band = near.get("loses_between");
    assertEquals(3.2, band.get("low").asDouble(), 1e-12);
    assertFalse(band.get("low_included").asBoolean());
    assertEquals(3.5, band.get("high").asDouble(), 1e-12);
    assertTrue(band.get("high_included").asBoolean());
    // The index grows 3.0% a year without noise: every draw of the forecast rounds to 3.0.
    assertEquals(3.0, near.get("forecast").get("median").asDouble(), 1e-12);
    assertEquals(0, near.get("p_loss").asDouble(), 1e-12);
    // Polymarket's own quotes on the band: the middle of Above 3.2 less that of Above 3.5.
    assertEquals(0.09, near.get("market_p_loss").asDouble(), 1e-9);
    assertEquals(1 - cost, near.get("expected_profit").asDouble(), 1e-4);
    assertEquals((1 - cost) / cost, near.get("expected_yield").asDouble(), 1e-4);
    assertEquals((1 - cost) / cost * 365 / 41.5625,
        near.get("annualized_expected_simple_365d").asDouble(), 1e-2);
    assertEquals("verified", near.get("same_quantity").asText());
    assertEquals("CUUR0000SA0 yoy_pct", near.get("settles_on").asText());
    assertNull(near.get("floor_profit"));
    assertTrue(out.get("next").asText().contains("MUST NOT report a near_locks entry as a "
        + "lock"), out.get("next").asText());
  }

  @Test void aNearLockIsSizedWhileASetIsExpectedToPayMoreThanItCosts() throws Exception {
    FakeFetcher f = venues(polymarketWide());
    // No on Kalshi is bought from its Yes bids: 0.80 x 20, then 0.85 x 100.
    kalshiBook(f, KALSHI_ID + "-T3.2", new double[] {0.20, 20, 0.15, 100},
        new double[] {0.24, 30});
    polymarketBook(f, "hi", true, 0.10, 500);
    ArrayNode asks = (ArrayNode) f.byPrefix.get(MarketHistory.CLOB + "/book?token_id=yes-hi")
        .get("asks");
    asks.addObject().put("price", "0.12").put("size", "50");
    asks.addObject().put("price", "0.30").put("size", "100");
    JsonNode near = MAPPER.readTree(scan(f, 60_000L).scan(args("{'search': 2}")))
        .get("near_locks").get(0);

    double first = 0.80 + 0.07 * 0.80 * 0.20 + 0.12;
    double second = 0.85 + 0.07 * 0.85 * 0.15 + 0.12;
    double capital = 20 * first + 30 * second;
    JsonNode size = near.get("size");
    assertEquals(20, size.get("sets_at_best_price").asDouble(), 1e-9);
    assertEquals(50, size.get("sets_with_a_positive_expected_profit").asDouble(), 1e-9);
    assertEquals(capital, size.get("capital").asDouble(), 0.005);
    assertEquals(50 - capital, size.get("expected_profit").asDouble(), 0.005);
    assertEquals(-capital, size.get("worst_profit").asDouble(), 0.005);
    assertEquals((50 - capital) / capital, size.get("expected_yield").asDouble(), 1e-4);
    assertNull(size.get("floor_profit"));
    // The third level pairs 0.85 on Kalshi with 0.30 on Polymarket: over the 1 expected.
    assertTrue(size.get("stops_because").asText().startsWith("the next set costs 1.1589 and "
        + "is expected to pay 1"), size.get("stops_because").asText());
    JsonNode flows = size.get("cashflows");
    assertEquals(capital, flows.get("paid_at_purchase").asDouble(), 0.005);
    assertEquals(0, flows.get("received_at_settlement_at_least").asDouble(), 1e-9);
    assertEquals(50, flows.get("received_at_settlement_expected").asDouble(), 0.005);
  }

  @Test void aBandTheForecastLandsInIsNotANearLock() throws Exception {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "lo", "Above 2.9%", 0.72, 0.74);
    JsonNode out = run(ev, "{'search': 2}");

    // YES above 3.0 on Kalshi with NO above 2.9 on Polymarket costs 0.737 and loses only at
    // 3.0 exactly, which is where the forecast lands.
    assertEquals(0, out.get("near_locks").size());
    JsonNode not = out.get("near_locks_not_scored").get(0);
    assertTrue(not.get("why").asText().startsWith("no basket of the pair profits in both "
        + "tails"), not.get("why").asText());
    assertEquals(1, not.get("count").asInt());
    assertFalse(out.get("next").asText().contains("near_locks"));
  }

  @Test void aBasketThatNeedsTheForecastAgainstAQuoteIsNotANearLock() throws Exception {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "lo", "Above 2.9%", 0.72, 0.74);
    polymarketMarket((ArrayNode) ev.get("markets"), "hi", "Above 3.3%", 0.24, 0.30);
    JsonNode out = run(ev, "{'search': 2}");

    // NO above 3.0 on Kalshi at 0.60 with YES above 3.3 on Polymarket at 0.30 loses only
    // between them and never on the forecast, which puts the first at 1 and the second at 0.
    assertEquals(0, out.get("near_locks").size());
    JsonNode not = out.get("near_locks_not_scored").get(0);
    assertTrue(not.get("why").asText().startsWith("the forecast favours a basket of the "
        + "pair, but puts a leg's chance of winning more than 0.2 from its price"),
        not.get("why").asText());
    assertEquals(1, not.get("count").asInt());
  }

  @Test void aBandTheQuotesCallLikelyIsNotANearLock() throws Exception {
    // Polymarket's quotes put 0.295 - 0.11 on the band the forecast never lands in.
    JsonNode out = run(polymarketWide(0.25, 0.34), "{'search': 2}");

    assertEquals(0, out.get("baskets").size());
    assertEquals(0, out.get("near_locks").size());
    JsonNode not = out.get("near_locks_not_scored").get(0);
    assertTrue(not.get("why").asText().startsWith("the forecast favours a basket of the "
        + "pair, but the quotes put more than max_loss_probability on its losing band"),
        not.get("why").asText());
  }

  @Test void aBandNoVenueQuotesIsNotANearLock() throws Exception {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "lo", "Above 2.9%", 0.72, 0.74);
    polymarketMarket((ArrayNode) ev.get("markets"), "hi", "Above 3.5%", 0.10, 0.12);
    // NO above 3.2 on Kalshi with YES above 3.5 on Polymarket: Kalshi lists no 3.5 and
    // Polymarket no 3.2, so neither says how likely the band between them is.
    JsonNode out = run(ev, "{'search': 2}");

    assertEquals(0, out.get("near_locks").size());
    JsonNode not = out.get("near_locks_not_scored").get(0);
    assertTrue(not.get("why").asText().startsWith("the forecast favours a basket of the "
        + "pair, but neither venue's quotes price its losing band"), not.get("why").asText());
  }

  @Test void aPairWithALockIsNotForecast() throws Exception {
    FakeFetcher f = venues(polymarketAbove(0.60));
    JsonNode out = MAPPER.readTree(scan(f, 60_000L).scan(args("{}")));

    assertEquals(1, out.get("baskets").size());
    assertEquals(0, out.get("near_locks").size());
    assertEquals(0, out.get("funnel").get("cross_venue_pairs_forecast").asInt());
    assertEquals(0, f.sqlReads);
  }

  @Test void aForecastThatCannotBeBuiltIsSaidAndLeavesTheScanComplete() throws Exception {
    FakeFetcher f = venues(polymarketWide());
    MarketBasketScan s = new MarketBasketScan(f,
        new PredictionMarkets.ListingCache(f, Duration.ofMinutes(15)), (q, limit) -> {
          throw new IllegalStateException("warehouse is down");
        }, () -> NOW, 10_000L, 60_000L, Duration.ofMinutes(15));
    JsonNode out = MAPPER.readTree(s.scan(args("{'search': 2}")));

    assertEquals("complete", out.get("status").asText());
    assertEquals(0, out.get("near_locks").size());
    JsonNode not = out.get("near_locks_not_scored").get(0);
    assertTrue(not.get("why").asText().startsWith("the forecast of the pair's quantity could "
        + "not be built: warehouse is down"), not.get("why").asText());
    assertEquals(KALSHI_ID, not.get("examples").get(0).asText().substring(7, 7
        + KALSHI_ID.length()));
  }

  @Test void forecastsWaitForTheNextCallOnceTheBudgetIsSpent() throws Exception {
    FakeFetcher f = venues(polymarketWide());
    MarketBasketScan s = scan(f, -1L);
    // The first call reads one event, the second the other: neither has time for a forecast.
    assertEquals("scanning", MAPPER.readTree(s.scan(args("{'search': 2}"))).get("status")
        .asText());
    JsonNode second = MAPPER.readTree(s.scan(args("{'search': 2}")));
    assertEquals("scanning", second.get("status").asText());
    assertEquals(0, f.sqlReads);
    assertTrue(second.get("next").asText().contains("1 pairs still to forecast"),
        second.get("next").asText());
    // With both events kept, the third call builds the forecast.
    JsonNode third = MAPPER.readTree(s.scan(args("{'search': 2}")));
    assertEquals("complete", third.get("status").asText());
    assertEquals(1, third.get("near_locks").size());
  }

  @Test void maxLossProbabilityMustBeUnderOneHalf() {
    assertThrows(IllegalArgumentException.class,
        () -> run(polymarketWide(), "{'max_loss_probability': 0.5}"));
  }

  @Test void aNearLockScanCarriesItsPanelsAndFollowsUpItsTopBasket() throws Exception {
    String raw = scan(venues(polymarketWide()), 60_000L).scan(args("{'search': 2}"));
    MarketPresentation shown = new MarketPresentation();
    JsonNode out = MAPPER.readTree(shown.basketScan(raw));

    boolean tile = false;
    boolean ranked = false;
    for (JsonNode title : out.get("dashboard_panels")) {
      tile |= "Near-locks found".equals(title.asText());
      ranked |= title.asText().startsWith("Near-locks by expected yield");
    }
    assertTrue(tile && ranked, out.get("dashboard_panels").toString());
    ObjectNode board = MAPPER.createObjectNode().put("layout",
        out.get("dashboard_layout").asText());
    assertTrue(shown.resolve(board));
    boolean quoted = false;
    for (JsonNode panel : board.get("panels")) {
      quoted |= "loss: 0.0% forecast, 9.0% quoted".equals(panel.path("delta").asText());
    }
    assertTrue(quoted, board.get("panels").toString());
    JsonNode followUps = out.get("follow_ups");
    assertEquals("price_market_event", followUps.get(0).get("tool").asText());
    assertEquals(KALSHI_ID, followUps.get(0).get("arguments").get("event_id").asText());
  }

  private static MarketPricing.Leg leg(String source, String id, String side, double strike,
      double price) {
    MarketPricing.Leg l = new MarketPricing.Leg();
    l.source = source;
    l.eventId = source + "-event";
    l.id = id;
    l.title = id;
    l.side = side;
    l.price = price;
    l.fee = 0;
    l.column = "v";
    l.condition = new MarketPricing.Condition("above", strike, strike);
    return l;
  }

  /** Kalshi above 3.0, Polymarket above 3.2 and above 3.0, each on both sides. */
  private static java.util.List<MarketPricing.Leg> nearLegs(double yes, double no) {
    return java.util.Arrays.asList(
        leg("kalshi", "k", "no", 3.0, 0.60), leg("kalshi", "k", "yes", 3.0, 0.42),
        leg("polymarket", "p", "yes", 3.2, 0.25), leg("polymarket", "p", "no", 3.2, 0.77),
        leg("polymarket", "p0", "yes", 3.0, yes), leg("polymarket", "p0", "no", 3.0, no));
  }

  @Test void aNearLockIsKeptOnlyWhileTheForecastsLossIsUnderTheCap() {
    // NO above 3.0 at 0.60 with YES above 3.2 at 0.25: both lose at 3.1 and 3.2 only. The
    // forecast puts the first at 0.90, 0.30 over its price, and the second at 0.05.
    // Polymarket quotes above 3.0 at a middle of 0.29 and above 3.2 at 0.24: 0.05 between.
    java.util.List<MarketPricing.Leg> legs = nearLegs(0.30, 0.72);
    double[] draws = new double[20];
    java.util.Arrays.fill(draws, 3.0);
    draws[0] = 3.1;
    draws[1] = 3.3;
    MarketPricing.Forecast forecast = MarketPricing.samples(draws);

    MarketPricing.NearLocks scored = MarketPricing.nearLocks(legs, forecast, 2, 2, 0.05,
        0.30, 2, 5);
    assertEquals(1, scored.kept.size());
    assertEquals(0, scored.overGap + scored.bandLikely + scored.bandUnpriced);
    JsonNode near = scored.kept.get(0);
    assertEquals("k", near.get("legs").get(0).get("id").asText());
    assertEquals("p", near.get("legs").get(1).get("id").asText());
    assertEquals(0.05, near.get("market_p_loss").asDouble(), 1e-9);
    assertEquals(0.90, near.get("legs").get(0).get("forecast_p_win").asDouble(), 1e-9);
    assertEquals(0.05, near.get("legs").get(1).get("forecast_p_win").asDouble(), 1e-9);
    assertEquals(0.30, near.get("max_quote_gap").asDouble(), 1e-9);
    assertEquals(1.5, near.get("max_quote_ratio").asDouble(), 1e-9);
    assertEquals(0.05, near.get("p_loss").asDouble(), 1e-9);
    assertEquals(0.95, near.get("p_profit").asDouble(), 1e-9);
    assertEquals(0.95 - 0.85, near.get("expected").asDouble(), 1e-9);
    assertEquals(-0.85, near.get("worst").asDouble(), 1e-9);
    assertEquals(0.15, near.get("best").asDouble(), 1e-9);
    assertEquals(3.0, near.get("loses_between").get("low").asDouble(), 1e-12);
    assertFalse(near.get("loses_between").get("low_included").asBoolean());
    assertEquals(3.2, near.get("loses_between").get("high").asDouble(), 1e-12);
    assertTrue(near.get("loses_between").get("high_included").asBoolean());

    MarketPricing.NearLocks risky = MarketPricing.nearLocks(legs, forecast, 2, 2, 0.04,
        0.30, 2, 5);
    assertEquals(0, risky.kept.size());
    assertEquals(0, risky.overGap + risky.bandLikely + risky.bandUnpriced);
    MarketPricing.NearLocks disputed = MarketPricing.nearLocks(legs, forecast, 2, 2, 0.05,
        0.20, 2, 5);
    assertEquals(0, disputed.kept.size());
    assertEquals(1, disputed.overGap);
    // The forecast puts the Kalshi leg at 0.90 where its price says 0.60: 1.5 times.
    MarketPricing.NearLocks stretched = MarketPricing.nearLocks(legs, forecast, 2, 2, 0.05,
        0.30, 1.4, 5);
    assertEquals(0, stretched.kept.size());
    assertEquals(1, stretched.overGap);
    // Above 3.0 quoted at a middle of 0.39 on Polymarket: 0.15 on the band, over the cap.
    MarketPricing.NearLocks likely = MarketPricing.nearLocks(nearLegs(0.40, 0.62), forecast,
        2, 2, 0.05, 0.30, 2, 5);
    assertEquals(0, likely.kept.size());
    assertEquals(1, likely.bandLikely);
    // Without Polymarket's market above 3.0 no venue quotes the band.
    MarketPricing.NearLocks unpriced = MarketPricing.nearLocks(nearLegs(0.40, 0.62)
        .subList(0, 4), forecast, 2, 2, 0.05, 0.30, 2, 5);
    assertEquals(0, unpriced.kept.size());
    assertEquals(1, unpriced.bandUnpriced);
  }

  @Test void aBasketThatLosesInATailOrLocksIsNotANearLock() {
    double[] draws = new double[20];
    java.util.Arrays.fill(draws, 3.0);
    MarketPricing.Forecast forecast = MarketPricing.samples(draws);
    // Two legs that both need a low print: nothing wins above 3.2.
    assertEquals(0, MarketPricing.nearLocks(java.util.Arrays.asList(
        leg("kalshi", "k", "no", 3.0, 0.40), leg("polymarket", "p", "no", 3.2, 0.45)),
        forecast, 2, 2, 0.10, 1, 100, 5).kept.size());
    // YES above 3.0 with NO above 3.2 pays 1 at every outcome and costs 0.90: a lock.
    assertEquals(0, MarketPricing.nearLocks(java.util.Arrays.asList(
        leg("kalshi", "k", "yes", 3.0, 0.40), leg("polymarket", "p", "no", 3.2, 0.50)),
        forecast, 2, 2, 0.10, 1, 100, 5).kept.size());
  }

  private static final String FED_ID = "KXFEDDECISION-26DEC";
  private static final String FED_POLY_ID = "9100";

  private static void fedKalshiMarket(ArrayNode markets, String suffix, String move,
      String month, String bid, String ask) {
    ObjectNode m = markets.addObject();
    m.put("ticker", FED_ID + "-" + suffix);
    m.put("status", "active");
    m.put("title", "Will the Federal Reserve " + move + " at their " + month
        + " meeting?");
    m.put("yes_bid_dollars", bid);
    m.put("yes_ask_dollars", ask);
    m.put("last_price_dollars", bid);
    m.put("volume_fp", "5000.00");
    m.put("volume_24h_fp", "800.00");
    m.put("open_interest_fp", "2000.00");
    m.put("close_time", "2026-12-09T19:00:00Z");
    m.put("strike_type", "custom");
    m.put("rules_primary", "Resolves on the change the Federal Reserve makes to the target "
        + "federal funds rate at the meeting.");
  }

  /** Kalshi's five outcomes of one meeting; "Hike >25bps" is bid 0.20 and asked 0.22. */
  private static ObjectNode fedKalshiEvent(String month) {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("title", "Fed decision in Dec 2026?");
    ev.put("event_ticker", FED_ID);
    ev.put("series_ticker", "KXFEDDECISION");
    ev.put("category", "Economics");
    ev.put("mutually_exclusive", true);
    ev.putArray("settlement_sources").addObject().put("name", "Federal Reserve");
    ArrayNode markets = ev.putArray("markets");
    fedKalshiMarket(markets, "C26", "Cut rates by >25bps", month, "0.0100", "0.0200");
    fedKalshiMarket(markets, "C25", "Cut rates by 25bps", month, "0.0200", "0.0300");
    fedKalshiMarket(markets, "H0", "Hike rates by 0bps", month, "0.2000", "0.2200");
    fedKalshiMarket(markets, "H25", "Hike rates by 25bps", month, "0.5000", "0.5200");
    fedKalshiMarket(markets, "H26", "Hike rates by >25bps", month, "0.2000", "0.2200");
    return ev;
  }

  private static void fedPolymarketMarket(ArrayNode markets, String id, String label,
      String question, double bid, double ask) {
    polymarketMarket(markets, id, label, bid, ask);
    ObjectNode m = (ObjectNode) markets.get(markets.size() - 1);
    m.put("question", question);
    m.put("endDate", "2026-12-09T19:00:00Z");
    m.put("description", "Resolves on the change of the upper bound of the target federal "
        + "funds rate at the meeting.");
  }

  /** Polymarket's five outcomes of the December meeting; "50+ bps increase" is asked
   *  {@code ask}. */
  private static ObjectNode fedPolymarketEvent(double ask) {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("id", FED_POLY_ID);
    ev.put("title", "Fed Decision in December?");
    ev.put("slug", "fed-decision-in-december");
    ev.put("negRisk", true);
    ev.putArray("tags").addObject().put("label", "Economy");
    ArrayNode markets = ev.putArray("markets");
    fedPolymarketMarket(markets, "d50", "50+ bps decrease", "Will the Fed decrease interest "
        + "rates by 50+ bps after the December 2026 meeting?", 0.01, 0.02);
    fedPolymarketMarket(markets, "d25", "25 bps decrease", "Will the Fed decrease interest "
        + "rates by 25 bps after the December 2026 meeting?", 0.02, 0.03);
    fedPolymarketMarket(markets, "n", "No change", "Will there be no change in Fed interest "
        + "rates after the December 2026 meeting?", 0.20, 0.22);
    fedPolymarketMarket(markets, "u25", "25 bps increase", "Will the Fed increase interest "
        + "rates by 25 bps after the December 2026 meeting?", 0.50, 0.52);
    fedPolymarketMarket(markets, "u50", "50+ bps increase", "Will the Fed increase interest "
        + "rates by 50+ bps after the December 2026 meeting?", ask - 0.02, ask);
    return ev;
  }

  private static JsonNode fedRun(ObjectNode kalshi, ObjectNode polymarket) throws Exception {
    FakeFetcher f = new FakeFetcher();
    ObjectNode series = MAPPER.createObjectNode();
    series.putObject("series").put("fee_type", "quadratic").put("fee_multiplier", 1);
    f.byPrefix.put(PredictionMarkets.KALSHI + "/series/", series);
    String kalshiId = kalshi.get("event_ticker").asText();
    ObjectNode one = MAPPER.createObjectNode();
    one.set("event", kalshi);
    f.byPrefix.put(PredictionMarkets.KALSHI + "/events/" + kalshiId, one);
    ObjectNode kList = MAPPER.createObjectNode();
    kList.putArray("events").add(kalshi);
    kList.put("cursor", "");
    f.byPrefix.put(PredictionMarkets.KALSHI + "/events?", kList);
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/" + FED_POLY_ID, polymarket);
    ObjectNode pList = MAPPER.createObjectNode();
    pList.putArray("events").add(polymarket);
    pList.put("next_cursor", "");
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/keyset?", pList);
    return MAPPER.readTree(scan(f, 60_000L).scan(args("{}")));
  }

  @Test void aFedDecisionOutcomeIsReadAsAChangeInBasisPoints() {
    PredictionMarkets.Market m = new PredictionMarkets.Market();
    m.title = "Will the Federal Reserve Cut rates by >25bps at their December 2026 meeting?";
    MarketPricing.Condition c = MarketPricing.Decision.outcome(m);
    assertEquals("below", c.kind);
    assertEquals(-25, c.low, 1e-12);
    m.title = "Will the Federal Reserve Hike rates by 0bps at their December 2026 meeting?";
    c = MarketPricing.Decision.outcome(m);
    assertEquals("between", c.kind);
    assertEquals(0, c.low, 1e-12);
    assertEquals(0, c.high, 1e-12);
    m.title = "Will the Federal Reserve hike rates by December 31, 2026?";
    assertNull(MarketPricing.Decision.outcome(m));

    m.label = "50+ bps decrease";
    c = MarketPricing.Decision.outcome(m);
    assertEquals("at_most", c.kind);
    assertEquals(-50, c.low, 1e-12);
    m.label = "25 bps increase";
    c = MarketPricing.Decision.outcome(m);
    assertEquals("between", c.kind);
    assertEquals(25, c.low, 1e-12);
    m.label = "No change";
    assertEquals(0, MarketPricing.Decision.outcome(m).high, 1e-12);
    m.label = "30 bps increase";
    assertNull(MarketPricing.Decision.outcome(m));
    m.label = "2 (50 bps)";
    assertNull(MarketPricing.Decision.outcome(m));
  }

  @Test void aSteppedGridScoresOnlyMultiplesOfTheStep() {
    java.util.List<MarketPricing.Leg> legs = java.util.Arrays.asList(
        leg("kalshi", "k", "no", 25, 0.80), leg("polymarket", "p", "yes", 50, 0.10));
    assertEquals(5, MarketPricing.grid(legs).rows.size());
    java.util.List<Map<String, Double>> rows = MarketPricing.grid(legs, 25.0).rows;
    assertEquals(4, rows.size());
    assertEquals(0.0, rows.get(0).get("v"), 1e-12);
    assertEquals(75.0, rows.get(3).get("v"), 1e-12);
    java.util.List<MarketPricing.Leg> off = java.util.Arrays.asList(
        leg("kalshi", "k", "no", 30, 0.80), leg("polymarket", "p", "yes", 50, 0.10));
    assertThrows(IllegalArgumentException.class, () -> MarketPricing.grid(off, 25.0));
  }

  @Test void aFedDecisionPairLocksOnTheChangeAtOneMeeting() throws Exception {
    // NO on Kalshi's "Hike >25bps" at 0.80 with YES on Polymarket's "50+ bps increase" at
    // 0.10: one of them pays at every multiple of 25, and both lose only at a change
    // between 25 and 50 that no meeting makes.
    JsonNode out = fedRun(fedKalshiEvent("December 2026"), fedPolymarketEvent(0.10));

    JsonNode funnel = out.get("funnel");
    assertEquals(1, funnel.get("cross_venue_pairs_priced").asInt(), out.toString());
    assertEquals(1, funnel.get("cross_venue_pairs_with_a_lock").asInt(), out.toString());
    assertEquals(2, funnel.get("events_with_conditions_read_from_labels").asInt());
    JsonNode lock = null;
    for (JsonNode b : out.get("baskets")) {
      if ("cross_venue".equals(b.get("type").asText()) && lock == null) {
        lock = b;
      }
    }
    assertNotNull(lock, out.toString());
    assertEquals("verified", lock.get("same_quantity").asText());
    assertTrue(lock.get("settles_on").asText().contains("December 2026 meeting"));
    assertTrue(lock.get("basis").asText().contains("multiple of 25 basis points"));
    double cost = 0.80 + 0.07 * 0.80 * 0.20 + 0.10;
    assertEquals(cost, lock.get("cost").asDouble(), 1e-3);
    assertEquals(1 - cost, lock.get("floor_profit").asDouble(), 1e-3);
    for (JsonNode leg : lock.get("legs")) {
      if ("kalshi".equals(leg.get("source").asText())) {
        assertEquals("no", leg.get("side").asText());
        assertEquals(25, leg.get("condition").get("above").asDouble(), 1e-12);
      } else {
        assertEquals("yes", leg.get("side").asText());
        assertEquals(50, leg.get("condition").get("at_least").asDouble(), 1e-12);
      }
    }
  }

  @Test void aFedDecisionPairWithNoGapIsPricedAndHoldsNoLock() throws Exception {
    JsonNode out = fedRun(fedKalshiEvent("December 2026"), fedPolymarketEvent(0.24));

    assertEquals(1, out.get("funnel").get("cross_venue_pairs_priced").asInt(), out.toString());
    assertEquals(0, out.get("funnel").get("cross_venue_pairs_with_a_lock").asInt());
    assertTrue(out.get("near_locks_not_scored").toString().contains("no forecast of an FOMC "
        + "decision"), out.toString());
  }

  @Test void decisionsOfDifferentMeetingsAreNotPriced() throws Exception {
    JsonNode out = fedRun(fedKalshiEvent("October 2026"), fedPolymarketEvent(0.10));

    assertEquals(0, out.get("funnel").get("cross_venue_pairs_priced").asInt(), out.toString());
    assertTrue(out.get("not_priced").get(0).get("why").asText().contains("different FOMC "
        + "meetings: October 2026 and December 2026"), out.toString());
  }

  @Test void aRateLevelIsNotPricedAgainstADecision() throws Exception {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("title", "Fed funds rate after the December 2026 meeting");
    ev.put("event_ticker", "KXFED-26DEC");
    ev.put("series_ticker", "KXFED");
    ev.put("category", "Economics");
    ev.putArray("settlement_sources").addObject().put("name", "Federal Reserve");
    ArrayNode markets = ev.putArray("markets");
    for (double strike : new double[]{0, 25}) {
      ObjectNode m = kalshiMarket("KXFED-26DEC-T" + strike, strike, "0.4000", "0.4400");
      m.put("title", "Will the upper bound of the federal funds rate be above " + strike
          + " after the December 2026 meeting?");
      m.put("close_time", "2026-12-09T19:00:00Z");
      m.put("rules_primary", "Resolves on the upper bound of the target federal funds rate "
          + "after the meeting.");
      markets.add(m);
    }
    JsonNode out = fedRun(ev, fedPolymarketEvent(0.10));

    assertEquals(0, out.get("funnel").get("cross_venue_pairs_priced").asInt(), out.toString());
    assertTrue(out.get("not_priced").get(0).get("why").asText().contains("not one quantity"),
        out.toString());
  }

  // ─── One month's change against twelve months' ──────────────────────────────

  /** The index a month-over-month and a year-over-year event share: up 0.2% and 0.3% in turn. */
  private static double[] turnIndex(int months) {
    double[] v = new double[months];
    v[0] = 100;
    for (int i = 1; i < months; i++) {
      v[i] = v[i - 1] * (i % 2 == 1 ? 1.002 : 1.003);
    }
    return v;
  }

  /**
   * A catalog holding one index from 2022-01 for {@code months} months under both its
   * seasonally adjusted id, by date, and its unadjusted id, by year and period.
   */
  private static MarketTools.SqlRunner indexSql(FakeFetcher f, int months) {
    return (q, limit) -> {
      f.sqlReads++;
      double[] v = turnIndex(months);
      ArrayNode rows = MAPPER.createArrayNode();
      YearMonth start = YearMonth.of(2022, 1);
      for (int i = 0; i < months; i++) {
        YearMonth ym = start.plusMonths(i);
        ObjectNode r = rows.addObject();
        if (q.contains("CPIAUCSL")) {
          r.put("date", ym.atDay(1).toString());
        } else {
          r.put("year", ym.getYear());
          r.put("period", String.format("M%02d", ym.getMonthValue()));
        }
        r.put("value", v[i]);
      }
      return rows;
    };
  }

  /** Polymarket's event on October's one-month change: Above 0.2% at 0.52 and 0.54. */
  private static ObjectNode polymarketMonthly() {
    ObjectNode ev = polymarketEvent();
    polymarketMarket((ArrayNode) ev.get("markets"), "mom", "Above 0.2%", 0.52, 0.54);
    ((ObjectNode) ev.get("markets").get(0)).put("description", "Resolves on the one-month "
        + "percent change in the seasonally adjusted BLS CPI-U, one decimal.");
    return ev;
  }

  private static JsonNode monthlyRun(int months) throws Exception {
    FakeFetcher f = venues(polymarketMonthly());
    MarketBasketScan scan = new MarketBasketScan(f,
        new PredictionMarkets.ListingCache(f, Duration.ofMinutes(15)), indexSql(f, months),
        () -> NOW, 10_000L, 60_000L, Duration.ofMinutes(15));
    return MAPPER.readTree(scan.scan(args("{'search': 2}")));
  }

  @Test void aMonthlyChangeAgainstATwelveMonthChangeIsConvertedAndIsNeverALock()
      throws Exception {
    // 57 months: the index is published to 2026-09, the month before the events'.
    JsonNode out = monthlyRun(57);

    assertEquals("complete", out.get("status").asText());
    JsonNode funnel = out.get("funnel");
    assertEquals(1, funnel.get("cross_venue_pairs_priced").asInt(), out.toString());
    assertEquals(1, funnel.get("cross_venue_pairs_converted").asInt());
    assertEquals(0, funnel.get("cross_venue_pairs_with_a_lock").asInt());
    assertEquals(0, out.get("baskets").size());
    assertEquals(1, out.get("near_locks").size(), out.toString());
    JsonNode near = out.get("near_locks").get(0);
    assertEquals("near_lock", near.get("type").asText());
    assertEquals("converted", near.get("same_quantity").asText());
    assertEquals("CPIAUCSL mom_pct and CUUR0000SA0 yoy_pct, 2026-10",
        near.get("settles_on").asText());

    double[] v = turnIndex(57);
    double ratio = v[56] / v[45];
    JsonNode conversion = near.get("conversion");
    assertEquals(ratio, conversion.get("index_ratio").asDouble(), 1e-6);
    assertEquals("CUUR0000SA0 in 2026-09 over 2025-10",
        conversion.get("index_ratio_is").asText());
    // Both ids hold one index: nothing between its two one-month changes in any month.
    assertEquals(0, conversion.get("wedge").asDouble(), 1e-9);
    assertEquals(0, conversion.get("wedge_error").get("max_abs").asDouble(), 1e-9);
    assertEquals(44, conversion.get("wedge_error").get("months").asInt());

    // YES above 3.0 on Kalshi at 0.44 plus its fee with NO above 0.2 on Polymarket at
    // 1 - 0.52. A month up 0.2% makes twelve months 3.0 and one up 0.3% makes them 3.1, so
    // one leg wins at every draw; a month up 0.21% to 0.25% publishes as 0.2 with twelve
    // months of 3.1, where both win. No error seen moves that, and the next one may: the
    // basket is listed as a near-lock with no losing band, never as a lock.
    double cost = 0.44 + 0.07 * 0.44 * 0.56 + 0.48;
    assertEquals(cost, near.get("cost").asDouble(), 1e-4);
    assertEquals(KALSHI_ID + "-T3.0", near.get("legs").get(0).get("market_id").asText());
    assertEquals("yes", near.get("legs").get(0).get("side").asText());
    assertEquals("mom", near.get("legs").get(1).get("market_id").asText());
    assertEquals("no", near.get("legs").get(1).get("side").asText());
    assertEquals(1 - cost, near.get("worst_profit").asDouble(), 1e-4);
    assertEquals(2 - cost, near.get("best_profit").asDouble(), 1e-4);
    assertTrue(near.get("loses_between").isNull());
    assertNull(near.get("floor_profit"));
    assertEquals(0, near.get("p_loss").asDouble(), 1e-12);
    assertEquals(0, near.get("market_p_loss").asDouble(), 1e-12);
    assertEquals(1 - cost, near.get("expected_profit").asDouble(), 1e-4);
    assertTrue(near.get("basis").asText().contains("two numbers"));
    assertTrue(out.get("next").asText().contains("wedge_error"), out.get("next").asText());
  }

  @Test void aConversionWaitsForTheMonthBeforeToPrint() throws Exception {
    // 56 months: the index stops at 2026-08, two months before the events'.
    JsonNode out = monthlyRun(56);

    assertEquals("complete", out.get("status").asText());
    assertEquals(1, out.get("funnel").get("cross_venue_pairs_converted").asInt());
    assertEquals(0, out.get("near_locks").size());
    String why = out.get("near_locks_not_scored").toString();
    assertTrue(why.contains("were not converted: CUUR0000SA0 has no value for 2026-09 yet"),
        why);
  }

  private static MarketForecasts.Quantity quantity(String series,
      MarketForecasts.Transform transform) {
    MarketForecasts.Quantity q = new MarketForecasts.Quantity(series + " " + transform.key,
        series, null);
    q.transform = transform;
    return q;
  }

  @Test void onlyOneIndexInTwoTransformsIsConverted() {
    MarketForecasts.Quantity m = quantity("CPIAUCSL", MarketForecasts.Transform.MOM);
    MarketForecasts.Quantity y = quantity("CUUR0000SA0", MarketForecasts.Transform.YOY);
    MarketForecasts.Quantity core = quantity("CPILFESL", MarketForecasts.Transform.MOM);
    MarketForecasts.Quantity level = quantity("CPIAUCSL", MarketForecasts.Transform.LEVEL);

    assertTrue(MarketForecasts.convertible(m, y));
    assertTrue(MarketForecasts.convertible(y, m));
    assertTrue(MarketForecasts.convertible(core,
        quantity("CUUR0000SA0L1E", MarketForecasts.Transform.YOY)));
    assertTrue(MarketForecasts.convertible(m,
        quantity("CPIAUCSL", MarketForecasts.Transform.YOY)));
    assertFalse(MarketForecasts.convertible(y, y), "one transform is one quantity");
    assertFalse(MarketForecasts.convertible(core, y), "core against headline is two indexes");
    assertFalse(MarketForecasts.convertible(level, y), "a level is not a percent change");
    assertFalse(MarketForecasts.convertible(new MarketForecasts.Quantity(null, "CPIAUCSL",
        "no transform"), y), "an unresolved event is not converted");
  }

  @Test void aConvertedBasketLosesOnlyWhereTheErrorCarriesOneNumberPastTheOther() {
    // The twelve-month change is 2.8 plus the month's less an error of -0.04, 0 or 0.04.
    // YES above 3.0 on it at 0.40 with NO above 0.2 on the month's at 0.50.
    java.util.List<MarketPricing.Leg> legs = java.util.Arrays.asList(
        leg("kalshi", "k", "yes", 3.0, 0.40), leg("kalshi", "k", "no", 3.0, 0.62),
        leg("polymarket", "p", "yes", 0.2, 0.52), leg("polymarket", "p", "no", 0.2, 0.50));
    double[] draws = new double[21];
    java.util.Arrays.fill(draws, 0, 10, 0.12);
    java.util.Arrays.fill(draws, 10, 20, 0.38);
    // Published as 0.3; the twelve months come to 3.03 and publish as 3.0 at an error of 0.04.
    draws[20] = 0.27;
    MarketPricing.Joint joint = new MarketPricing.Joint("polymarket", "month", "twelve",
        draws, new double[]{-0.04, 0, 0.04}, 2.8, 1.0, 1, 1);

    MarketPricing.NearLocks scored = MarketPricing.nearLocks(legs, joint, 2, 2, 0.05, 0.20,
        2, 5);
    assertEquals(1, scored.kept.size(), scored.kept.toString());
    assertEquals(0, scored.overGap + scored.bandLikely + scored.bandUnpriced);
    JsonNode near = scored.kept.get(0);
    assertEquals("k", near.get("legs").get(0).get("id").asText());
    assertEquals("yes", near.get("legs").get(0).get("side").asText());
    assertEquals("p", near.get("legs").get(1).get("id").asText());
    assertEquals(1.0 / 63, near.get("p_loss").asDouble(), 1e-4);
    assertEquals(62.0 / 63 - 0.90, near.get("expected").asDouble(), 1e-4);
    assertEquals(-0.90, near.get("worst").asDouble(), 1e-9);
    JsonNode band = near.get("loses_between");
    assertEquals("twelve", band.get("of").asText());
    assertEquals(3.0, band.get("low").asDouble(), 1e-12);
    assertEquals(3.0, band.get("high").asDouble(), 1e-12);
    assertEquals(0.3, band.get("and").get("low").asDouble(), 1e-12);
    assertEquals(0.3, band.get("and").get("high").asDouble(), 1e-12);
    // Kalshi quotes at or under 3.0 at a middle of 0.61, and 1 of the 31 draws there loses;
    // Polymarket quotes above 0.2 at 0.51, and 1 of the 33 draws there loses.
    assertEquals(0.61 / 31, near.get("market_p_loss").asDouble(), 1e-4);

    // A cap under the forecast's loss keeps nothing.
    assertEquals(0, MarketPricing.nearLocks(legs, joint, 2, 2, 0.01, 0.20, 2, 5).kept.size());
  }
}
