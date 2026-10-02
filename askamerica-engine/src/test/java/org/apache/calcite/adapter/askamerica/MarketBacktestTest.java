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
import java.time.Instant;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The backtest against canned venue responses and a canned CPI series: no network, no
 * catalog.
 *
 * <p>CPI rises a steady 0.25% a month, so the baseline forecast of the 12-month rate is 3.0
 * exactly. Its probabilities for the three strikes (above 2.8, 3.0, 3.2) are therefore 1, 0 and
 * 0; each event settles yes, no, no, so the baseline scores a Brier of 0 and the venue's
 * price scores whatever the fixture quotes. Events close on the 12th of the month after the
 * month they measure; with 20 days of lead the settlement month has not ended, so its row is
 * not yet known.
 */
@Tag("unit")
class MarketBacktestTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-10-02T00:00:00Z");
  private static final String K = PredictionMarkets.KALSHI;
  private static final double[] STRIKES = {2.8, 3.0, 3.2};

  /** Canned venue: a settled listing, event titles, and one candle before the moment asked
   *  for and one after it. */
  private static final class Venue implements PredictionMarkets.Fetcher {
    final List<ObjectNode> markets = new ArrayList<>();
    /** How many of {@link #markets}, from the first, the live tier still lists; the rest
     *  are past the venue's cutoff and only its historical tier serves them. */
    int liveMarkets = Integer.MAX_VALUE;
    final List<String> urls = new ArrayList<>();
    /** Yes price of a market ticker at the moment; null means no candle. */
    Function<String, Double> price = t -> 0.5;

    @Override public JsonNode get(String url) throws IOException {
      urls.add(url);
      boolean historical = url.startsWith(K + "/historical/");
      if (url.startsWith(K + "/markets?status=settled")
          || url.startsWith(K + "/historical/markets?series_ticker=")) {
        int live = Math.min(liveMarkets, markets.size());
        ObjectNode doc = MAPPER.createObjectNode();
        doc.set("markets", MAPPER.createArrayNode().addAll(historical
            ? markets.subList(live, markets.size()) : markets.subList(0, live)));
        doc.put("cursor", "");
        return doc;
      }
      if (url.startsWith(K + "/events/")) {
        String id = url.substring((K + "/events/").length());
        YearMonth ym = YearMonth.of(2000 + Integer.parseInt(id.substring(6, 8)),
            Integer.parseInt(id.substring(8, 10)));
        ObjectNode doc = MAPPER.createObjectNode();
        doc.putObject("event").put("title", "CPI inflation in "
            + ym.getMonth().name().charAt(0)
            + ym.getMonth().name().substring(1).toLowerCase(Locale.ROOT) + " " + ym.getYear());
        return doc;
      }
      if (url.contains("/candlesticks?")) {
        String ticker = url.substring(url.indexOf("/markets/") + 9, url.indexOf("/candlesticks"));
        if (historical != isHistorical(ticker)) {
          // Each tier answers 404 for a market the other one holds.
          throw new PredictionMarkets.HttpStatusException(404, url);
        }
        long end = Long.parseLong(url.replaceAll(".*end_ts=(\\d+).*", "$1"));
        ObjectNode doc = MAPPER.createObjectNode();
        ArrayNode c = doc.putArray("candlesticks");
        Double p = price.apply(ticker);
        if (p != null) {
          candle(c, historical, end - 7200, p, p - 0.01, p + 0.01);
          // A candle after the moment is a look-ahead; it must never be read.
          candle(c, historical, end + 3600, 0.01, 0.0, 0.02);
        }
        return doc;
      }
      throw new IOException("no canned response for " + url);
    }

    private boolean isHistorical(String ticker) {
      for (int i = 0; i < markets.size(); i++) {
        if (ticker.equals(markets.get(i).get("ticker").asText())) {
          return i >= liveMarkets;
        }
      }
      throw new IllegalStateException("no market " + ticker);
    }

    /** The historical tier names the same fields without the unit suffixes. */
    private static void candle(ArrayNode c, boolean historical, long end, double price,
        double bid, double ask) {
      String close = historical ? "close" : "close_dollars";
      ObjectNode n = c.addObject();
      n.put("end_period_ts", end);
      n.put(historical ? "volume" : "volume_fp", "10.00");
      n.putObject("price").put(close, String.valueOf(price));
      n.putObject("yes_bid").put(close, String.valueOf(bid));
      n.putObject("yes_ask").put(close, String.valueOf(ask));
    }
  }

  /** Adds the three markets of the event measuring {@code month}, settled yes, no, no. */
  private static void addEvent(Venue v, YearMonth month, String result0) {
    String id = String.format("KXCPI-%02d%02d", month.getYear() - 2000, month.getMonthValue());
    String close = month.plusMonths(1).atDay(12) + "T12:30:00Z";
    String[] results = {result0, "no", "no"};
    for (int i = 0; i < STRIKES.length; i++) {
      ObjectNode m = MAPPER.createObjectNode();
      m.put("ticker", id + "-T" + STRIKES[i]);
      m.put("event_ticker", id);
      m.put("title", "Will CPI be above " + STRIKES[i] + "%?");
      m.put("result", results[i]);
      m.put("expiration_value", "3.1");
      m.put("close_time", close);
      m.put("settlement_ts", close);
      m.put("last_price_dollars", "0.5000");
      m.put("strike_type", "greater");
      m.put("floor_strike", STRIKES[i]);
      m.put("subtitle", String.valueOf(STRIKES[i]));
      m.put("rules_primary", "Resolves on the BLS CPI-U 12-month change, one decimal.");
      v.markets.add(m);
    }
  }

  /** Nine settled events, most recent first: January to September 2026. */
  private static Venue venueWithEvents(int count) {
    Venue v = new Venue();
    for (int i = count - 1; i >= 0; i--) {
      addEvent(v, YearMonth.of(2026, 1).plusMonths(i), "yes");
    }
    return v;
  }

  private static ArrayNode cpiRows() {
    ArrayNode rows = MAPPER.createArrayNode();
    YearMonth start = YearMonth.of(2024, 1);
    for (int i = 0; i < 36; i++) {
      YearMonth ym = start.plusMonths(i);
      ObjectNode r = rows.addObject();
      r.put("year", ym.getYear());
      r.put("period", String.format("M%02d", ym.getMonthValue()));
      r.put("value", 100 * Math.pow(1.0025, i));
    }
    return rows;
  }

  private static MarketBacktest backtest(Venue v) {
    return new MarketBacktest(v, (q, limit) -> cpiRows(), () -> NOW);
  }

  private static JsonNode args(String json) throws Exception {
    return MAPPER.readTree(json.replace('\'', '"'));
  }

  private static JsonNode run(Venue v, String json) throws Exception {
    return MAPPER.readTree(backtest(v).backtestTool(args(json)));
  }

  /** Quotes the 3.0 and 3.2 strikes a little away from their outcome, the 2.8 strike at 0.8,
   *  varying with the event so the Brier difference has a spread. */
  private static Function<String, Double> noisyPrices() {
    return t -> {
      int month = Integer.parseInt(t.substring(8, 10));
      double base = t.endsWith("T2.8") ? 0.8 : t.endsWith("T3.0") ? 0.4 : 0.2;
      return base + (t.endsWith("T2.8") ? -0.01 : 0.01) * month;
    };
  }

  @Test void aBaselineThatBeatsTheQuotedPriceIsReportedWithItsStandardError()
      throws Exception {
    Venue v = venueWithEvents(9);
    v.price = noisyPrices();
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20}");
    assertEquals(9, out.get("events_scored").asInt(), out.toString());
    assertEquals(27, out.get("markets_scored").asInt());
    assertEquals(0.0, out.get("brier_baseline").asDouble(), 1e-9);
    assertTrue(out.get("brier_price").asDouble() > 0.05, out.toString());
    assertTrue(out.get("brier_difference").asDouble() < 0);
    assertTrue(out.get("brier_difference_se").asDouble() > 0);
    assertEquals("baseline_beats_market", out.get("verdict").asText(), out.toString());
    assertTrue(out.get("baseline_beats_market").asBoolean());
    assertEquals(1.0, out.get("hit_rate_baseline").asDouble(), 1e-9);
    assertEquals(1.0, out.get("hit_rate_price").asDouble(), 1e-9);
    assertEquals(1.0, out.get("baseline_closer_share").asDouble(), 1e-9);
    assertTrue(out.get("revision_caveat").asText().contains("current revised values"));
    assertTrue(out.get("next").asText().contains("skill"), out.get("next").asText());
    assertEquals(0, out.get("skipped").size());
  }

  @Test void eventsPastTheVenuesCutoffAreReadFromItsHistoricalTier() throws Exception {
    Venue v = venueWithEvents(9);
    v.price = noisyPrices();
    // Only the two most recent events are still on the live tier.
    v.liveMarkets = 2 * STRIKES.length;
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20}");
    assertEquals(9, out.get("events_scored").asInt(), out.toString());
    assertEquals(27, out.get("markets_scored").asInt());
    assertEquals("baseline_beats_market", out.get("verdict").asText(), out.toString());
    long historicalCandles = v.urls.stream()
        .filter(u -> u.startsWith(K + "/historical/markets/") && u.contains("/candlesticks?"))
        .count();
    long liveCandles = v.urls.stream()
        .filter(u -> u.startsWith(K + "/series/KXCPI/markets/")).count();
    assertEquals(21, historicalCandles, v.urls.toString());
    assertEquals(6, liveCandles, v.urls.toString());
  }

  @Test void eachEventListsItsCloseAsOfMedianSettlementAndBothScores() throws Exception {
    Venue v = venueWithEvents(9);
    v.price = noisyPrices();
    JsonNode e = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20}")
        .get("events").get(0);
    assertEquals("KXCPI-2609", e.get("event_id").asText());
    assertEquals("2026-10-12T12:30:00Z", e.get("close_time").asText());
    assertEquals("2026-09-22", e.get("as_of").asText());
    assertEquals("2026-09-22T12:30:00Z", e.get("price_taken_at").asText());
    assertEquals(3.0, e.get("forecast_median").asDouble(), 1e-9);
    assertEquals("3.1", e.get("settlement_value").asText());
    assertEquals("CUUR0000SA0", e.get("forecast_series").asText());
    assertEquals(0.0, e.get("baseline_brier").asDouble(), 1e-9);
    assertTrue(e.get("price_brier").asDouble() > 0);
    assertEquals(3, e.get("markets_scored").asInt());
  }

  @Test void thePriceIsTheCandleAtOrBeforeTheMomentNeverALaterOne() throws Exception {
    Venue v = venueWithEvents(9);
    v.price = t -> t.endsWith("T2.8") ? 0.7 : t.endsWith("T3.0") ? 0.3 : 0.1;
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20}");
    // The poisoned later candle quotes 0.01 on every strike: reading it would give the 2.8
    // strike a squared error of 0.98.
    double expected = ((0.3 * 0.3) + (0.3 * 0.3) + (0.1 * 0.1)) / 3;
    assertEquals(expected, out.get("brier_price").asDouble(), 1e-4);
  }

  @Test void aPriceThatBeatsTheBaselineIsReportedAsMarketBeatsBaseline() throws Exception {
    Venue v = venueWithEvents(9);
    // Settled results flip for the 3.0 strike, which the baseline gives 0: the baseline is
    // wrong on it in every event while the price is right.
    for (ObjectNode m : v.markets) {
      if (m.get("ticker").asText().endsWith("T3.0")) {
        m.put("result", "yes");
      }
    }
    v.price = t -> {
      int month = Integer.parseInt(t.substring(8, 10));
      return (t.endsWith("T3.2") ? 0.02 : 0.95) - 0.001 * month;
    };
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20}");
    assertEquals("market_beats_baseline", out.get("verdict").asText(), out.toString());
    assertFalse(out.get("baseline_beats_market").asBoolean());
    assertTrue(out.get("brier_difference").asDouble() > 0);
    // The baseline is exact on two of the three strikes, the price on the third.
    assertEquals(2.0 / 3, out.get("baseline_closer_share").asDouble(), 1e-3);
    assertEquals(1.0 / 3, out.get("price_closer_share").asDouble(), 1e-3);
    assertTrue(out.get("next").asText().contains("not as an edge"));
  }

  @Test void fewerThanEightEventsIsInconclusive() throws Exception {
    Venue v = venueWithEvents(7);
    v.price = noisyPrices();
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20}");
    assertEquals(7, out.get("events_scored").asInt());
    assertEquals("inconclusive", out.get("verdict").asText());
    assertTrue(out.get("baseline_beats_market").isNull());
    assertTrue(out.get("verdict_reason").asText().contains("only 7 events"),
        out.get("verdict_reason").asText());
    assertTrue(out.get("next").asText().contains("inconclusive"));
  }

  @Test void aDifferenceWithinTwoStandardErrorsIsInconclusive() throws Exception {
    Venue v = venueWithEvents(9);
    // In odd months the 3.0 strike settles yes, which the baseline gives 0, and the price is
    // exact; in even months the price is poor and the baseline exact. The mean difference is
    // small against its spread.
    for (ObjectNode m : v.markets) {
      int month = Integer.parseInt(m.get("ticker").asText().substring(8, 10));
      if (month % 2 == 1 && m.get("ticker").asText().endsWith("T3.0")) {
        m.put("result", "yes");
      }
    }
    v.price = t -> {
      boolean odd = Integer.parseInt(t.substring(8, 10)) % 2 == 1;
      return t.endsWith("T2.8") ? (odd ? 0.99 : 0.30) : t.endsWith("T3.0")
          ? (odd ? 0.99 : 0.70) : 0.01;
    };
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20}");
    double diff = out.get("brier_difference").asDouble();
    double se = out.get("brier_difference_se").asDouble();
    assertTrue(Math.abs(diff) <= 2 * se, out.toString());
    assertEquals("inconclusive", out.get("verdict").asText(), out.toString());
    assertTrue(out.get("verdict_reason").asText().contains("standard errors"));
  }

  @Test void anEventWhoseSettlementMonthIsAlreadyInTheCatalogIsSkippedWithTheReason()
      throws Exception {
    Venue v = venueWithEvents(9);
    // CPI prints 16 days after its month ends. A market left open to the 25th is, 7 days
    // before its close, past that release: the settlement row is known.
    for (ObjectNode m : v.markets) {
      String late = m.get("close_time").asText().replace("-12T", "-25T");
      m.put("close_time", late);
      m.put("settlement_ts", late);
    }
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI'}");
    assertEquals(0, out.get("events_scored").asInt());
    assertEquals(9, out.get("skipped").size());
    assertTrue(out.get("skipped").get(0).get("reason").asText().contains("settlement period"),
        out.get("skipped").get(0).toString());
    assertEquals("inconclusive", out.get("verdict").asText());
    assertTrue(out.get("brier_baseline").isNull());
    assertTrue(out.get("next").asText().contains("days_before"));
  }

  @Test void aMarketWithoutAYesOrNoResultIsLeftOutAndTheEventStillScored() throws Exception {
    Venue v = venueWithEvents(9);
    v.price = noisyPrices();
    for (ObjectNode m : v.markets) {
      if (m.get("ticker").asText().equals("KXCPI-2605-T3.2")) {
        m.put("result", "scalar");
      }
    }
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20}");
    assertEquals(9, out.get("events_scored").asInt(), out.toString());
    assertEquals(26, out.get("markets_scored").asInt());
    JsonNode may = null;
    for (JsonNode e : out.get("events")) {
      if ("KXCPI-2605".equals(e.get("event_id").asText())) {
        may = e;
      }
    }
    assertEquals(2, may.get("markets_scored").asInt());
    assertEquals(1, may.get("markets_skipped").get("settled with result 'scalar', not yes or no")
        .asInt(), may.toString());
  }

  @Test void aMarketWithNoQuoteIsLeftOutAndAnEventWithNoPriceIsSkipped() throws Exception {
    Venue v = venueWithEvents(9);
    Function<String, Double> noisy = noisyPrices();
    v.price = t -> t.startsWith("KXCPI-2603") ? null : t.equals("KXCPI-2604-T3.2") ? null
        : noisy.apply(t);
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20}");
    assertEquals(8, out.get("events_scored").asInt(), out.toString());
    assertEquals(1, out.get("skipped").size());
    assertEquals("KXCPI-2603", out.get("skipped").get(0).get("event_id").asText());
    assertTrue(out.get("skipped").get(0).get("reason").asText().contains("no market had a price"),
        out.get("skipped").get(0).toString());
    assertEquals(23, out.get("markets_scored").asInt());
  }

  @Test void eventsAskedForLimitsHowManyAreScored() throws Exception {
    Venue v = venueWithEvents(9);
    v.price = noisyPrices();
    JsonNode out = run(v,
        "{'source':'kalshi','series':'KXCPI','days_before':20,'events':3}");
    assertEquals(3, out.get("events_read").asInt());
    assertEquals(3, out.get("events_scored").asInt());
    assertEquals("KXCPI-2609", out.get("events").get(0).get("event_id").asText());
  }

  @Test void forecastArgsReachTheBuilderAndReservedKeysAreRejected() throws Exception {
    Venue v = venueWithEvents(9);
    v.price = noisyPrices();
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20,"
        + "'forecast_args':{'round':2}}");
    assertEquals(3.04, out.get("events").get(0).get("forecast_median").asDouble(), 1e-9);
    assertThrows(IllegalArgumentException.class, () -> run(v,
        "{'source':'kalshi','series':'KXCPI','forecast_args':{'as_of':'2026-01-01'}}"));
    assertThrows(IllegalArgumentException.class, () -> run(v,
        "{'source':'kalshi','series':'KXCPI','forecast_args':{'bogus':1}}"));
  }

  @Test void argumentsAreChecked() {
    Venue v = venueWithEvents(1);
    assertThrows(IllegalArgumentException.class, () -> run(v, "{'source':'kalshi'}"));
    assertThrows(IllegalArgumentException.class,
        () -> run(v, "{'source':'polymarket','series':'x'}"));
    assertThrows(IllegalArgumentException.class,
        () -> run(v, "{'source':'kalshi','series':'x','days_before':0}"));
    assertThrows(IllegalArgumentException.class,
        () -> run(v, "{'source':'kalshi','series':'x','events':41}"));
    assertThrows(IllegalArgumentException.class,
        () -> run(v, "{'source':'kalshi','series':'x','days_before':'7'}"));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> run(v, "{'source':'kalshi','series':'x','as_of':'2026-01-01'}"));
    assertTrue(e.getMessage().contains("unknown key 'as_of'"), e.getMessage());
  }

  @Test void anEventWhoseMarketsTheListingMayHaveCutOffIsSkippedNotScored()
      throws Exception {
    Venue v = new Venue();
    // One event of 41 markets; events=1 reads at most 40, so the last event read is partial.
    for (int i = 0; i < 41; i++) {
      ObjectNode m = MAPPER.createObjectNode();
      m.put("ticker", "KXCPI-2609-T" + i);
      m.put("event_ticker", "KXCPI-2609");
      m.put("title", "Will CPI be above " + i + "?");
      m.put("result", "no");
      m.put("expiration_value", "3.1");
      m.put("close_time", "2026-10-12T12:30:00Z");
      m.put("settlement_ts", "2026-10-12T12:30:00Z");
      m.put("last_price_dollars", "0.5000");
      m.put("strike_type", "greater");
      m.put("floor_strike", 5.0 + i);
      v.markets.add(m);
    }
    JsonNode out = run(v, "{'source':'kalshi','series':'KXCPI','days_before':20,'events':1}");
    assertEquals(0, out.get("events_scored").asInt());
    assertTrue(out.get("skipped").get(0).get("reason").asText().contains("cut off"),
        out.toString());
  }

  @Test void theToolDefinitionFitsAndAdvertisesExactlyTheAcceptedKeys() {
    ObjectNode def = MarketBacktest.toolDef();
    assertEquals(MarketBacktest.TOOL, def.get("name").asText());
    String description = def.get("description").asText();
    assertTrue(description.length() <= 2048, "description is " + description.length());
    assertTrue(description.contains("You MUST"));
    Set<String> advertised = new TreeSet<>();
    Iterator<String> names = def.get("inputSchema").get("properties").fieldNames();
    while (names.hasNext()) {
      advertised.add(names.next());
    }
    assertEquals(MarketBacktest.KEYS, advertised);
    assertEquals(2, def.get("inputSchema").get("required").size());
  }

  // ─── Records and the confidence tier ───────────────────────────────────────

  @Test void aBacktestOfTheEnginesOwnForecastLeavesARecordAndAnOverrideDoesNot()
      throws Exception {
    Venue v = venueWithEvents(9);
    v.price = noisyPrices();
    MarketBacktest b = backtest(v);
    assertNull(b.record("kalshi", "KXCPI"));
    b.backtestTool(args("{'source':'kalshi','series':'KXCPI','days_before':20,"
        + "'forecast_args':{'round':2}}"));
    assertNull(b.record("kalshi", "KXCPI"), "an override scores another forecast");
    JsonNode out = MAPPER.readTree(b.backtestTool(
        args("{'source':'kalshi','series':'KXCPI','days_before':20}")));
    JsonNode r = b.record("kalshi", "KXCPI");
    assertEquals("baseline_beats_market", r.get("verdict").asText());
    assertEquals(9, r.get("events_scored").asInt());
    assertEquals(20, r.get("days_before").asInt());
    assertEquals(out.get("brier_difference").asDouble(), r.get("brier_difference").asDouble(),
        1e-12);
    assertEquals(NOW.toString(), r.get("run_at").asText());
    assertNull(b.record("kalshi", "KXOTHER"));
    assertNull(b.record("kalshi", null));
  }

  private static ObjectNode recordWith(String verdict) {
    ObjectNode r = MAPPER.createObjectNode();
    r.put("verdict", verdict);
    r.put("verdict_reason", "why");
    return r;
  }

  @Test void confidenceIsBacktestedOnlyForABuiltUnflaggedForecastThatBeatThePrice()
      throws Exception {
    JsonNode none = args("[]");
    JsonNode ok = MarketBacktest.confidence(recordWith("baseline_beats_market"), none, true,
        "kalshi", "KXCPI");
    assertEquals("backtested", ok.get("tier").asText());
    assertEquals(0, ok.get("reasons").size());
    assertFalse(ok.has("must"));
    assertEquals("baseline_beats_market", ok.get("backtest").get("verdict").asText());

    JsonNode unrun = MarketBacktest.confidence(null, none, true, "kalshi", "KXCPI");
    assertEquals("weak", unrun.get("tier").asText());
    assertTrue(unrun.get("backtest").isNull());
    assertTrue(unrun.get("must").asText().startsWith(
        "You MUST call backtest_market_forecast(source='kalshi', series='KXCPI')"));

    JsonNode poly = MarketBacktest.confidence(null, none, true, "polymarket", "cpi");
    assertEquals("weak", poly.get("tier").asText());
    assertTrue(poly.get("reasons").get(0).asText().contains("Kalshi series only"));
    assertFalse(poly.get("must").asText().contains("backtest_market_forecast"));

    for (String verdict : new String[] {"market_beats_baseline", "inconclusive"}) {
      JsonNode c = MarketBacktest.confidence(recordWith(verdict), none, true, "kalshi",
          "KXCPI");
      assertEquals("weak", c.get("tier").asText());
      assertTrue(c.get("reasons").get(0).asText().contains(verdict + ": why"), c.toString());
    }

    JsonNode given = MarketBacktest.confidence(recordWith("baseline_beats_market"), null,
        false, "kalshi", "KXCPI");
    assertEquals("weak", given.get("tier").asText());
    assertTrue(given.get("reasons").get(0).asText().contains("given by the caller"));

    // Flags arrive as codes from the scan and as objects from the builder; only a blocking
    // one lowers the tier.
    JsonNode flagged = MarketBacktest.confidence(recordWith("baseline_beats_market"),
        args("['history_stale',{'code':'rounding_mismatch'},'short_history']"), true, "kalshi",
        "KXCPI");
    assertEquals("weak", flagged.get("tier").asText());
    assertEquals(2, flagged.get("reasons").size(), flagged.toString());
  }
}
