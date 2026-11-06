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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Market discovery beyond the economy: elections, disasters, health, law and fiscal events are
 * listed with the corpus tables that could ground them, an event no table grounds is listed as
 * market only, and an election is priced only with a probability that names its source.
 * Venues are canned; no network, no catalog.
 */
@Tag("unit")
class MarketDomainsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-10-17T00:00:00Z");
  private static final String CLOSE = "2026-11-04T05:00:00Z";
  private static final String HOUSE_ID = "CONTROLH-2026";
  private static final String HOUSE_DEM = HOUSE_ID + "-D";
  private static final String HOUSE_REP = HOUSE_ID + "-R";

  /** Answers a URL from a table of exact-prefix handlers and remembers every URL asked. */
  private static final class StubFetcher implements PredictionMarkets.Fetcher {
    final List<String> asked = new ArrayList<>();
    final List<String[]> routes = new ArrayList<>();
    final List<JsonNode> bodies = new ArrayList<>();
    final Set<String> failing = new HashSet<>();

    void on(String prefix, JsonNode body) {
      routes.add(new String[]{prefix});
      bodies.add(body);
    }

    @Override public synchronized JsonNode get(String url) throws IOException {
      asked.add(url);
      for (String f : failing) {
        if (url.startsWith(f)) {
          throw new IOException("unreachable: " + url);
        }
      }
      for (int i = 0; i < routes.size(); i++) {
        if (url.startsWith(routes.get(i)[0])) {
          return bodies.get(i);
        }
      }
      throw new IOException("no canned response for " + url);
    }
  }

  private static ObjectNode kalshiMarket(String ticker, String sub, String bid, String ask) {
    ObjectNode m = MAPPER.createObjectNode();
    m.put("ticker", ticker);
    m.put("status", "active");
    m.put("title", "");
    m.put("yes_sub_title", sub);
    m.put("yes_bid_dollars", bid);
    m.put("yes_ask_dollars", ask);
    m.put("last_price_dollars", bid);
    m.put("volume_fp", "9000.00");
    m.put("volume_24h_fp", "700.00");
    m.put("open_interest_fp", "3000.00");
    m.put("close_time", CLOSE);
    m.put("rules_primary", "Resolves to the party holding a majority of the House.");
    return m;
  }

  private static ObjectNode kalshiEvent(String ticker, String series, String category,
      String title, ObjectNode... markets) {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("title", title);
    ev.put("event_ticker", ticker);
    ev.put("series_ticker", series);
    ev.put("category", category);
    ev.put("mutually_exclusive", true);
    ArrayNode arr = ev.putArray("markets");
    for (ObjectNode m : markets) {
      arr.add(m);
    }
    return ev;
  }

  /** A Kalshi book: resting No bids and Yes bids. */
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

  private static ObjectNode oneMarketKalshi(String ticker, String series, String category,
      String title) {
    return kalshiEvent(ticker, series, category, title,
        kalshiMarket(ticker + "-Y", "Yes", "0.4000", "0.4400"));
  }

  private static ObjectNode houseControl() {
    return kalshiEvent(HOUSE_ID, "CONTROLH", "Elections",
        "Which party will win the House in 2026?",
        kalshiMarket(HOUSE_DEM, "Democratic", "0.7000", "0.7400"),
        kalshiMarket(HOUSE_REP, "Republican", "0.2600", "0.3000"));
  }

  private static ObjectNode polymarketEvent(String id, String title, String tag, String market) {
    ObjectNode ev = MAPPER.createObjectNode();
    ev.put("id", id);
    ev.put("title", title);
    ev.put("slug", "slug-" + id);
    ev.putArray("tags").addObject().put("label", tag);
    ObjectNode m = ev.putArray("markets").addObject();
    m.put("id", market);
    m.put("question", title);
    m.put("active", true);
    m.put("closed", false);
    m.put("outcomes", "[\"Yes\",\"No\"]");
    m.put("outcomePrices", "[\"0.45\",\"0.55\"]");
    m.put("bestBid", 0.44);
    m.put("bestAsk", 0.46);
    m.put("volumeNum", 30000);
    m.put("volume24hr", 2000);
    m.put("endDate", CLOSE);
    m.put("feesEnabled", false);
    m.put("description", "Resolves on the certified result.");
    return ev;
  }

  /** Both venues, each with events in every domain and one nothing grounds. */
  private static StubFetcher venues() {
    StubFetcher f = new StubFetcher();
    ObjectNode series = MAPPER.createObjectNode();
    series.putObject("series").put("fee_type", "quadratic").put("fee_multiplier", 1);
    f.on(PredictionMarkets.KALSHI + "/series/", series);
    ObjectNode one = MAPPER.createObjectNode();
    one.set("event", houseControl());
    f.on(PredictionMarkets.KALSHI + "/events/" + HOUSE_ID, one);
    ObjectNode kList = MAPPER.createObjectNode();
    ArrayNode events = kList.putArray("events");
    events.add(houseControl());
    events.add(oneMarketKalshi("HOUSE-CA13", "HOUSECA", "Elections", "CA-13 House winner?"));
    events.add(oneMarketKalshi("FEMA-OCT", "FEMAD", "Climate and Weather",
        "Will FEMA issue a major disaster declaration in Florida in 2026?"));
    events.add(oneMarketKalshi("MEASLES", "MEASLES", "Health", "Measles cases in 2026?"));
    events.add(oneMarketKalshi("KOSA", "KOSA", "Politics",
        "Will the Kids Online Safety Act become law?"));
    events.add(oneMarketKalshi("SHUT", "SHUT", "Politics", "Government shutdown on Dec 12, 2026?"));
    events.add(oneMarketKalshi("UFC", "UFC", "Politics", "Will Donald Trump attend UFC 333?"));
    events.add(oneMarketKalshi("DROUGHT", "TEAM", "Sports",
        "New York J: To Break Playoff Drought"));
    events.add(oneMarketKalshi("BRAZIL", "BRGOV", "Elections", "Amazonas Governor winner?"));
    kList.put("cursor", "");
    f.on(PredictionMarkets.KALSHI + "/events?", kList);

    ObjectNode page1 = MAPPER.createObjectNode();
    page1.putArray("events").add(
        polymarketEvent("7001", "Wyoming Senate Election Winner", "Politics", "pm-wy"));
    page1.put("next_cursor", "page2");
    ObjectNode page2 = MAPPER.createObjectNode();
    ArrayNode p2 = page2.putArray("events");
    p2.add(polymarketEvent("7002", "Will a hurricane make landfall in Hawaii before 2027?",
        "Other", "pm-hi"));
    p2.add(polymarketEvent("7003", "US population above 340 million by 2027?", "Other",
        "pm-pop"));
    p2.add(polymarketEvent("7004", "Will Mojtaba Khamenei leave Iran by December?", "World",
        "pm-ir"));
    page2.put("next_cursor", "");
    f.on(PredictionMarkets.POLYMARKET + "/events/keyset?limit=100&closed=false&order=volume24hr"
        + "&ascending=false&exclude_tag_id=1&after_cursor=page2", page2);
    f.on(PredictionMarkets.POLYMARKET + "/events/keyset?", page1);
    // The order books the ticket of a mispriced market reads, then the market documents.
    f.on(PredictionMarkets.KALSHI + "/markets/" + HOUSE_DEM + "/orderbook",
        kalshiBook(new String[][]{{"0.2600", "400.00"}}, new String[][]{{"0.7000", "200.00"}}));
    f.on(PredictionMarkets.KALSHI + "/markets/" + HOUSE_REP + "/orderbook",
        kalshiBook(new String[][]{{"0.7000", "400.00"}}, new String[][]{{"0.2600", "200.00"}}));
    for (JsonNode m : houseControl().get("markets")) {
      ObjectNode doc = MAPPER.createObjectNode();
      ObjectNode market = ((ObjectNode) m).deepCopy();
      market.put("event_ticker", HOUSE_ID);
      doc.set("market", market);
      f.on(PredictionMarkets.KALSHI + "/markets/" + m.get("ticker").asText(), doc);
    }
    ObjectNode pm = ((ObjectNode) polymarketEvent("7001", "Wyoming Senate Election Winner",
        "Politics", "pm-wy").get("markets").get(0)).deepCopy();
    pm.put("acceptingOrders", true);
    pm.put("clobTokenIds", "[\"yes1\", \"no1\"]");
    f.on(PredictionMarkets.POLYMARKET + "/markets/pm-wy", pm);
    ObjectNode polyBook = MAPPER.createObjectNode();
    polyBook.putArray("bids").addObject().put("price", "0.44").put("size", "400");
    polyBook.putArray("asks").addObject().put("price", "0.46").put("size", "250");
    f.on(MarketHistory.CLOB + "/book?token_id=yes1", polyBook);
    f.on(PredictionMarkets.POLYMARKET + "/events/7001",
        polymarketEvent("7001", "Wyoming Senate Election Winner", "Politics", "pm-wy"));
    return f;
  }

  private static MarketTools tools(PredictionMarkets.Fetcher fetcher) {
    return new MarketTools(fetcher,
        new PredictionMarkets.ListingCache(fetcher, Duration.ofMinutes(15)),
        (q, limit) -> {
          throw new AssertionError("no query expected: " + q);
        }, () -> NOW, 10_000L);
  }

  private static JsonNode args(String json) throws Exception {
    return MAPPER.readTree(json.replace('\'', '"'));
  }

  private static JsonNode eventTitled(JsonNode out, String title) {
    for (JsonNode e : out.get("events")) {
      if (title.equals(e.get("event_title").asText())) {
        return e;
      }
    }
    throw new AssertionError("no event titled '" + title + "' in "
        + out.get("events").findValuesAsText("event_title"));
  }

  // ─── Classification ────────────────────────────────────────────────────────

  @Test void eachDomainHasADriverAndItsTables() {
    String[][] cases = {
        {"Which party will win the House in 2026?", "elections", "chamber_control",
            "officials.state_political_index"},
        {"CA-13 House winner?", "elections", "district_race", "fec.candidate_district_profile"},
        {"California's 13th District margin of victory", "elections", "district_race",
            "officials.current_members"},
        {"Texas 7 House General Election: voter turnout", "elections", "voter_turnout",
            "census.cps_voting_supplement"},
        {"Wyoming Senate Election Winner", "elections", "statewide_race",
            "officials.state_political_index"},
        {"New Mexico Senate Election Winner", "elections", "statewide_race",
            "officials.state_political_index"},
        {"Will Democrats win the US presidential election?", "elections",
            "presidential_election", "officials.presidential_election_results"},
        {"Will FEMA issue a major disaster declaration in Florida?", "disasters",
            "disaster_declaration", "disasters.disaster_declarations"},
        {"Will a hurricane make landfall in Hawaii before 2027?", "disasters",
            "hurricane_landfall", "disasters.storm_events"},
        {"Acres burned by wildfires in California in 2026?", "disasters", "wildfire",
            "disasters.wildfire_perimeters"},
        {"Measles cases in 2026?", "demographics_health", "outbreak",
            "health.cdc_nndss_weekly"},
        {"US population above 340 million by 2027?", "demographics_health", "population",
            "census.pep_population"},
        {"Will the Kids Online Safety Act become law?", "legal", "legislation",
            "law.bills"},
        {"Supreme Court vacancy in 2026?", "legal", "supreme_court", "law.scotus_cases"},
        {"Government shutdown on Dec 12, 2026?", "fiscal", "shutdown",
            "fiscal.omb_apportionments"},
        {"US trade deficit and federal spending in 2026?", "fiscal", "deficit",
            "econ.federal_outlays_by_function"},
        {"Will CPI rise above 3%?", "economy", "inflation", "econ.inflation_metrics"},
    };
    for (String[] c : cases) {
      PredictionMarkets.Driver d = PredictionMarkets.driverOf(c[0]);
      assertNotNull(d, c[0]);
      assertEquals(c[1], d.domain, c[0]);
      assertEquals(c[2], d.name, c[0]);
      assertTrue(d.tables.contains(c[3]), c[0] + " -> " + d.tables);
      assertTrue(d.grounded(), c[0]);
    }
  }

  @Test void ungroundedAndForeignEventsHaveNoDriver() {
    assertNull(PredictionMarkets.driverOf("Will Donald Trump attend UFC 333?"));
    assertNull(PredictionMarkets.driverOf("Amazonas Governor winner?"));
    assertNull(PredictionMarkets.driverOf("Guinea-Bissau Presidential Election"));
    assertNull(PredictionMarkets.driverOf("Spanish general election: 2nd place"));
    assertNull(PredictionMarkets.driverOf("ECB rate cut in 2026?"));
    assertNull(PredictionMarkets.driverOf("Brazil presidential runoff: which countries win?"));
  }

  @Test void anOutcomeDriverIsKeptWhereTheOldFilterDroppedTheTitle() {
    // "who will" and "nominee" marked a title as not a measurement; a race is an outcome the
    // election tables inform, so these are kept, while a statement about a person still is not.
    assertEquals("statewide_race",
        PredictionMarkets.driverOf("Who will win the Ohio Senate race?").name);
    assertEquals("outcome",
        PredictionMarkets.driverOf("Who will win the Ohio Senate race?").basis);
    assertNull(PredictionMarkets.driverOf("Who will the Fed nominee be?"));
    assertNull(PredictionMarkets.driverOf("Will Trump say tariff on Truth Social?"));
  }

  @Test void everyDriverCarriesADomainAndABasis() {
    for (PredictionMarkets.Driver d : PredictionMarkets.DRIVERS) {
      assertTrue(PredictionMarkets.DOMAINS.contains(d.domain), d.name);
      assertTrue(PredictionMarkets.BASES.contains(d.basis), d.name);
      assertFalse(PredictionMarkets.NONE.equals(d.domain), d.name);
    }
  }

  // ─── Discovery ─────────────────────────────────────────────────────────────

  @Test void polymarketIsPagedInFullWithoutATagAllowList() throws Exception {
    StubFetcher f = venues();
    JsonNode out = MAPPER.readTree(tools(f).findCandidates(args("{'within':400}")));
    List<String> polymarketUrls = new ArrayList<>();
    for (String u : f.asked) {
      if (u.startsWith(PredictionMarkets.POLYMARKET + "/events/keyset?")) {
        polymarketUrls.add(u);
      }
    }
    assertEquals(2, polymarketUrls.size(), polymarketUrls.toString());
    for (String u : polymarketUrls) {
      assertFalse(u.contains("tag_slug"), u);
      assertTrue(u.contains("exclude_tag_id=" + PredictionMarkets.POLYMARKET_SPORTS_TAG), u);
    }
    assertTrue(polymarketUrls.get(1).contains("after_cursor=page2"));
    assertEquals(2, out.get("listing").get("polymarket_pages").asInt());
    assertEquals(4, out.get("listing").get("polymarket_markets_read").asInt());
    assertFalse(out.get("listing").get("coverage").asText().contains("tagged"));
  }

  @Test void candidatesCoverElectionsDisastersHealthLawAndFiscalOnBothVenues()
      throws Exception {
    JsonNode out = MAPPER.readTree(tools(venues()).findCandidates(args("{'within':400}")));
    JsonNode byDomain = out.get("matched_by_domain");
    assertEquals(3, byDomain.get("elections").asInt());
    assertEquals(2, byDomain.get("disasters").asInt());
    assertEquals(2, byDomain.get("demographics_health").asInt());
    assertEquals(1, byDomain.get("legal").asInt());
    assertEquals(1, byDomain.get("fiscal").asInt());
    assertFalse(byDomain.has("market_only"));
    assertEquals(2, out.get("matched_by_basis").get("climatology").asInt());
    assertEquals(5, out.get("matched_by_basis").get("outcome").asInt());
    assertEquals(2, out.get("matched_by_basis").get("release").asInt());
    assertTrue(out.get("matched_by_venue").get("polymarket").asInt() >= 3);
    // The Brazilian race, the UFC event and the Iran event are no election the tables ground.
    assertEquals(3, out.get("market_only_not_shown").asInt());
    JsonNode house = eventTitled(out, "Which party will win the House in 2026?");
    assertEquals("kalshi", house.get("source").asText());
    assertEquals("elections", house.get("domain").asText());
    assertEquals("outcome", house.get("basis").asText());
    assertTrue(house.get("grounded").asBoolean());
    List<String> tables = new ArrayList<>();
    house.get("govdata_tables").forEach(t -> tables.add(t.asText()));
    assertTrue(tables.contains("officials.state_political_index"), tables.toString());
    assertTrue(tables.contains("fec.candidate_summaries"), tables.toString());
    assertTrue(house.get("forecast_with").asText().contains("never from the market's own price"));
    JsonNode senate = eventTitled(out, "Wyoming Senate Election Winner");
    assertEquals("polymarket", senate.get("source").asText());
    assertEquals("elections", senate.get("domain").asText());
    JsonNode hurricane = eventTitled(out, "Will a hurricane make landfall in Hawaii before 2027?");
    assertEquals("disasters", hurricane.get("domain").asText());
    assertEquals("climatology", hurricane.get("basis").asText());
  }

  @Test void anEventNoTableGroundsIsMarketOnlyAndNeverForecast() throws Exception {
    JsonNode all = MAPPER.readTree(tools(venues()).findCandidates(args("{'within':400}")));
    int notShown = all.get("market_only_not_shown").asInt();
    assertEquals(3, notShown, "UFC, the Brazilian race and the Iran event");
    for (JsonNode e : all.get("events")) {
      assertFalse("market_only".equals(e.get("domain").asText()));
    }
    JsonNode out = MAPPER.readTree(tools(venues()).findCandidates(
        args("{'within':400,'domain':'market_only'}")));
    assertEquals(notShown, out.get("matched").asInt());
    assertFalse(out.has("market_only_not_shown"));
    JsonNode ufc = eventTitled(out, "Will Donald Trump attend UFC 333?");
    assertEquals("market_only", ufc.get("domain").asText());
    assertEquals("market_only", ufc.get("basis").asText());
    assertFalse(ufc.get("grounded").asBoolean());
    assertEquals(0, ufc.get("govdata_tables").size());
    assertTrue(ufc.get("forecast_with").asText().startsWith("none"));
    for (JsonNode e : out.get("events")) {
      assertNotNull(e.get("event_id"));
      assertFalse(e.get("grounded").asBoolean());
    }
  }

  @Test void theDomainFilterKeepsOnlyThatDomain() throws Exception {
    JsonNode out = MAPPER.readTree(tools(venues()).findCandidates(
        args("{'within':400,'domain':'elections'}")));
    assertEquals(3, out.get("matched").asInt());
    for (JsonNode e : out.get("events")) {
      assertEquals("elections", e.get("domain").asText());
    }
    JsonNode both = out.get("matched_by_venue");
    assertEquals(2, both.get("kalshi").asInt());
    assertEquals(1, both.get("polymarket").asInt());
    assertThrows(IllegalArgumentException.class,
        () -> tools(venues()).findCandidates(args("{'domain':'astrology'}")));
  }

  @Test void aFailedSecondPolymarketPageFailsTheListing() {
    StubFetcher f = venues();
    f.failing.add(PredictionMarkets.POLYMARKET + "/events/keyset?limit=100&closed=false"
        + "&order=volume24hr&ascending=false&exclude_tag_id=1&after_cursor=page2");
    assertThrows(IOException.class,
        () -> tools(f).findCandidates(args("{'within':400}")));
  }

  // ─── Pricing an election ───────────────────────────────────────────────────

  private static String houseSpec(String extra) {
    return "{'source':'kalshi','event_id':'" + HOUSE_ID + "'" + extra + "}";
  }

  @Test void anElectionWithNoForecastReturnsQuotesAndTheTablesNotAnEdge() throws Exception {
    JsonNode out = MAPPER.readTree(tools(venues()).priceEvent(args(houseSpec(""))));
    assertEquals("outcome", out.get("basis").asText());
    assertEquals("elections", out.get("domain").asText());
    assertTrue(out.get("forecast").isNull());
    assertEquals(0, out.get("priced_markets").size());
    assertEquals(2, out.get("markets_without_condition").size());
    String next = out.get("next").asText();
    assertTrue(next.contains("probability_source"), next);
    assertTrue(next.contains("MUST NOT take the probability from the market's own price"), next);
    assertFalse(next.contains("build_forecast=true"), next);
  }

  @Test void buildForecastRefusesADiscreteOutcomeInsteadOfInventingOne() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> tools(venues()).priceEvent(args(houseSpec(",'build_forecast':true"))));
    assertTrue(e.getMessage().contains("discrete outcome"), e.getMessage());
    assertTrue(e.getMessage().contains("probability_source"), e.getMessage());
    assertTrue(e.getMessage().contains("officials.state_political_index"), e.getMessage());
  }

  @Test void aProbabilityWithoutItsSourceIsNotPriced() {
    String cond = ",'conditions':{'" + HOUSE_DEM + "':{'above':0.5}}";
    IllegalArgumentException noSource = assertThrows(IllegalArgumentException.class,
        () -> tools(venues()).priceEvent(args(houseSpec(",'probability':0.8" + cond))));
    assertTrue(noSource.getMessage().contains("probability_source"), noSource.getMessage());
    assertThrows(IllegalArgumentException.class, () -> tools(venues()).priceEvent(
        args(houseSpec(",'probability':0.8,'probability_source':'  '" + cond))));
    assertThrows(IllegalArgumentException.class, () -> tools(venues()).priceEvent(
        args(houseSpec(",'probability_source':'my model'" + cond))));
    assertThrows(IllegalArgumentException.class, () -> tools(venues()).priceEvent(
        args(houseSpec(",'probability':1.2,'probability_source':'my model'" + cond))));
    assertThrows(IllegalArgumentException.class, () -> tools(venues()).priceEvent(
        args(houseSpec(",'probability':0.8,'probability_source':'my model'"))));
  }

  @Test void aSourcedProbabilityPricesTheNamedMarketsNetOfTheFee() throws Exception {
    String cond = ",'conditions':{'" + HOUSE_DEM + "':{'above':0.5},'" + HOUSE_REP
        + "':{'below':0.5}},'min_edge':0.03";
    JsonNode out = MAPPER.readTree(tools(venues()).priceEvent(args(houseSpec(
        ",'probability':0.85,'probability_source':'cross-validated fundamentals model, "
        + "officials.state_political_index + econ.state_economic_snapshot'" + cond))));
    assertTrue(out.get("forecast").get("form").asText().contains("source: cross-validated"));
    JsonNode dem = null;
    JsonNode rep = null;
    for (JsonNode m : out.get("priced_markets")) {
      if (HOUSE_DEM.equals(m.get("market_id").asText())) {
        dem = m;
      } else {
        rep = m;
      }
    }
    assertNotNull(dem);
    assertNotNull(rep);
    assertEquals(0.85, dem.get("fair").asDouble(), 1e-6);
    assertEquals(0.15, rep.get("fair").asDouble(), 1e-6);
    // YES on Democratic costs the 0.74 ask plus the fee; the forecast says 0.85.
    double fee = 0.07 * 0.74 * 0.26;
    assertEquals("yes", dem.get("side").asText());
    assertEquals(0.85 - 0.74 - fee, dem.get("edge").asDouble(), 1e-3);
    assertEquals("mispriced", dem.get("verdict").asText());
    assertEquals("elections", out.get("domain").asText());
  }

  @Test void anElectionOnPolymarketIsPricedTheSameWay() throws Exception {
    JsonNode out = MAPPER.readTree(tools(venues()).priceEvent(args(
        "{'source':'polymarket','event_id':'7001','probability':0.30,"
        + "'probability_source':'published ratings, cross-checked against fec.candidate_summaries',"
        + "'conditions':{'pm-wy':{'above':0.5}}}")));
    assertEquals("statewide_race", out.get("driver").asText());
    JsonNode m = out.get("priced_markets").get(0);
    assertEquals(0.30, m.get("fair").asDouble(), 1e-6);
    assertEquals("no", m.get("side").asText());
  }

  @Test void anElectionFindsItsCounterpartOnTheOtherVenue() throws Exception {
    StubFetcher f = venues();
    ObjectNode kList = MAPPER.createObjectNode();
    ArrayNode events = kList.putArray("events");
    events.add(oneMarketKalshi("KXWY", "KXWY", "Elections", "Wyoming Senate winner?"));
    kList.put("cursor", "");
    f.bodies.set(indexOf(f, PredictionMarkets.KALSHI + "/events?"), kList);
    MarketTools t = tools(f);
    JsonNode out = MAPPER.readTree(t.priceEvent(args(
        "{'source':'polymarket','event_id':'7001'}")));
    JsonNode other = out.get("other_venue");
    assertEquals("kalshi", other.get("venue").asText());
    assertEquals("found", other.get("status").asText());
    assertEquals("KXWY", other.get("counterparts").get(0).get("event_id").asText());
  }

  private static int indexOf(StubFetcher f, String prefix) {
    for (int i = 0; i < f.routes.size(); i++) {
      if (f.routes.get(i)[0].equals(prefix)) {
        return i;
      }
    }
    throw new AssertionError(prefix);
  }
}
