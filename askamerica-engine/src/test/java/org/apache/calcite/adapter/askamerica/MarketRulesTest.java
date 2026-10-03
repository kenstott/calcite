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
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The settlement-rules diff against canned venue responses: no network. Two Kalshi events
 * stand in for the two sides, since the diff reads only the event's text and strike types.
 */
@Tag("unit")
class MarketRulesTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CLOSE = "2026-10-14T12:30:00Z";
  private static final String TITLE = "CPI month-over-month for September 2026";
  private static final String BASE = "If the seasonally adjusted CPI-U (CUSR0000SA0) change "
      + "from the previous month for September 2026, rounded to one decimal place, is above "
      + "0.3%, the market resolves Yes. The source is the Bureau of Labor Statistics. The "
      + "first release is used; later revisions are not considered.";

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

  /** Registers a Kalshi event whose one market carries {@code rules}. */
  private static void add(FakeFetcher f, String id, String title, String rules,
      String strikeType) {
    ObjectNode m = MAPPER.createObjectNode();
    m.put("ticker", id + "-T0.3");
    m.put("status", "active");
    m.put("title", title);
    m.put("yes_bid_dollars", "0.4000");
    m.put("yes_ask_dollars", "0.4400");
    m.put("last_price_dollars", "0.4000");
    m.put("volume_fp", "5000.00");
    m.put("volume_24h_fp", "800.00");
    m.put("open_interest_fp", "2000.00");
    m.put("close_time", CLOSE);
    m.put("strike_type", strikeType);
    m.put("floor_strike", 0.3);
    m.put("rules_primary", rules);
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("title", title);
    ev.put("event_ticker", id);
    ev.put("series_ticker", "KXTEST");
    ev.put("category", "Economics");
    ev.putArray("markets").add(m);
    ObjectNode one = MAPPER.createObjectNode();
    one.set("event", ev);
    f.byPrefix.put(PredictionMarkets.KALSHI + "/events/" + id, one);
  }

  private static FakeFetcher venues(String titleB, String rulesB, String strikeB) {
    FakeFetcher f = new FakeFetcher();
    ObjectNode series = MAPPER.createObjectNode();
    series.putObject("series").put("fee_type", "quadratic").put("fee_multiplier", 1);
    f.byPrefix.put(PredictionMarkets.KALSHI + "/series/KXTEST", series);
    add(f, "EV-A", TITLE, BASE, "greater");
    add(f, "EV-B", titleB, rulesB, strikeB);
    return f;
  }

  private static JsonNode compare(FakeFetcher f) throws Exception {
    ObjectNode args = MAPPER.createObjectNode();
    args.putObject("a").put("source", "kalshi").put("event_id", "EV-A");
    args.putObject("b").put("source", "kalshi").put("event_id", "EV-B");
    return MAPPER.readTree(new MarketRules(f).compareSettlementRules(args));
  }

  private static void assertDiffers(JsonNode out, String dimension) {
    assertEquals("differ", out.get("rules_match").asText());
    assertEquals("differ", out.at("/dimensions/" + dimension + "/status").asText());
    assertTrue(out.get("differing").toString().contains(dimension), out.get("differing")
        .toString());
    assertTrue(out.get("next").asText().contains("not a lock"));
  }

  @Test void identicalRulesMatch() throws Exception {
    JsonNode out = compare(venues(TITLE, BASE, "greater"));
    assertEquals("match", out.get("rules_match").asText(), out.toString());
    assertEquals(0, out.get("differing").size());
    assertEquals(0, out.get("unknown").size());
    for (String d : MarketRules.DIMENSIONS) {
      assertEquals("match", out.at("/dimensions/" + d + "/status").asText(), d);
    }
    assertEquals("BLS", out.at("/dimensions/source_agency/a").asText());
    assertEquals("CUSR0000SA0", out.at("/dimensions/series/a").asText());
    assertEquals("2026-09", out.at("/dimensions/settlement_period/a").asText());
    assertEquals("month-over-month", out.at("/dimensions/transform/a").asText());
    assertEquals("1 decimals", out.at("/dimensions/rounding/a").asText());
    assertEquals("first print", out.at("/dimensions/revision_handling/a").asText());
    assertEquals("strictly beyond threshold", out.at("/dimensions/tie_handling/a").asText());
  }

  @Test void differentSeasonalAdjustmentDiffers() throws Exception {
    JsonNode out = compare(venues(TITLE,
        BASE.replace("seasonally adjusted", "not seasonally adjusted"), "greater"));
    assertDiffers(out, "seasonal_adjustment");
  }

  @Test void differentPeriodDiffers() throws Exception {
    JsonNode out = compare(venues(TITLE.replace("September", "August"),
        BASE.replace("September", "August"), "greater"));
    assertDiffers(out, "settlement_period");
  }

  @Test void differentRoundingDiffers() throws Exception {
    JsonNode out = compare(venues(TITLE, BASE.replace("one decimal", "two decimal"),
        "greater"));
    assertDiffers(out, "rounding");
  }

  @Test void strictAgainstInclusiveThresholdDiffers() throws Exception {
    JsonNode out = compare(venues(TITLE, BASE.replace("is above", "is at or above"),
        "greater_or_equal"));
    assertDiffers(out, "tie_handling");
    assertEquals("at or beyond threshold", out.at("/dimensions/tie_handling/b").asText());
  }

  @Test void differentTransformDiffers() throws Exception {
    JsonNode out = compare(venues(TITLE.replace("month-over-month", "year-over-year"),
        BASE.replace("from the previous month", "year-over-year"), "greater"));
    assertDiffers(out, "transform");
  }

  /** Registers a Polymarket event whose one market carries {@code rules} and closes then. */
  private static void addPolymarket(FakeFetcher f, String id, String rules, String end) {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("id", id);
    ev.put("title", TITLE);
    ev.put("slug", "cpi-mom-september-2026");
    ev.putArray("tags").addObject().put("label", "Economy");
    ObjectNode m = ev.putArray("markets").addObject();
    m.put("id", "m1");
    m.put("question", "Will CPI rise more than 0.3% in September 2026?");
    m.put("active", true);
    m.put("closed", false);
    m.put("outcomes", "[\"Yes\",\"No\"]");
    m.put("outcomePrices", "[\"0.55\",\"0.45\"]");
    m.put("bestBid", 0.54);
    m.put("bestAsk", 0.56);
    m.put("volumeNum", 20000);
    m.put("volume24hr", 3000);
    m.put("endDate", end);
    m.put("feesEnabled", false);
    m.put("description", rules);
    f.byPrefix.put(PredictionMarkets.POLYMARKET + "/events/" + id, ev);
  }

  private static JsonNode compareAcrossVenues(String rules, String end) throws Exception {
    FakeFetcher f = venues(TITLE, BASE, "greater");
    addPolymarket(f, "9001", rules, end);
    ObjectNode args = MAPPER.createObjectNode();
    args.putObject("a").put("source", "kalshi").put("event_id", "EV-A");
    args.putObject("b").put("source", "polymarket").put("event_id", "9001");
    return MAPPER.readTree(new MarketRules(f).compareSettlementRules(args));
  }

  @Test void aPolymarketEventIsReadFromItsDescription() throws Exception {
    // Closes two days after the Kalshi event: one release.
    JsonNode out = compareAcrossVenues(BASE, "2026-10-16T12:00:00Z");
    assertEquals("polymarket", out.at("/b/source").asText(), out.toString());
    for (String d : new String[]{"source_agency", "series", "settlement_period", "transform",
        "seasonal_adjustment", "rounding", "release_or_close_date", "revision_handling"}) {
      assertEquals("match", out.at("/dimensions/" + d + "/status").asText(), d + " " + out);
    }
    assertEquals("2026-10-14", out.at("/dimensions/release_or_close_date/a").asText());
    assertEquals("2026-10-16", out.at("/dimensions/release_or_close_date/b").asText());
    assertEquals(0, out.get("differing").size(), out.toString());
  }

  @Test void closeDatesAReleaseApartDiffer() throws Exception {
    assertDiffers(compareAcrossVenues(BASE, "2026-11-13T12:00:00Z"), "release_or_close_date");
  }

  @Test void silentTextIsUnverified() throws Exception {
    JsonNode out = compare(venues("Will CPI be high?", "Resolves Yes if the figure is high.",
        "greater"));
    assertEquals("unverified", out.get("rules_match").asText(), out.toString());
    assertTrue(out.get("next").asText().contains("not a lock"));
    assertTrue(out.at("/dimensions/seasonal_adjustment/b").isNull());
    assertEquals("unknown", out.at("/dimensions/seasonal_adjustment/status").asText());
    assertEquals("unknown", out.at("/dimensions/source_agency/status").asText());
    assertTrue(out.get("unknown").toString().contains("rounding"));
    assertEquals(0, out.get("differing").size());
  }

  @Test void badArgumentsAreRejected() {
    MarketRules rules = new MarketRules(venues(TITLE, BASE, "greater"));
    ObjectNode noB = MAPPER.createObjectNode();
    noB.putObject("a").put("source", "kalshi").put("event_id", "EV-A");
    assertThrows(IllegalArgumentException.class, () -> rules.compareSettlementRules(noB));

    ObjectNode noId = MAPPER.createObjectNode();
    noId.putObject("a").put("source", "kalshi");
    noId.putObject("b").put("source", "kalshi").put("event_id", "EV-B");
    assertThrows(IllegalArgumentException.class, () -> rules.compareSettlementRules(noId));

    ObjectNode extra = MAPPER.createObjectNode();
    extra.putObject("a").put("source", "kalshi").put("event_id", "EV-A");
    extra.putObject("b").put("source", "kalshi").put("event_id", "EV-B");
    extra.put("c", "x");
    assertThrows(IllegalArgumentException.class, () -> rules.compareSettlementRules(extra));

    ObjectNode badSource = MAPPER.createObjectNode();
    badSource.putObject("a").put("source", "manifold").put("event_id", "EV-A");
    badSource.putObject("b").put("source", "kalshi").put("event_id", "EV-B");
    assertThrows(IllegalArgumentException.class, () -> rules.compareSettlementRules(badSource));

    ObjectNode notObject = MAPPER.createObjectNode();
    notObject.put("a", "EV-A");
    notObject.put("b", "EV-B");
    assertThrows(IllegalArgumentException.class, () -> rules.compareSettlementRules(notObject));
  }

  @Test void toolDescriptionIsUnderTheLimit() {
    ObjectNode def = MarketRules.toolDef();
    assertEquals("compare_settlement_rules", def.get("name").asText());
    int length = def.get("description").asText().length();
    assertTrue(length < 2048, "description is " + length + " characters");
    assertEquals("a", def.at("/inputSchema/required/0").asText());
  }
}
