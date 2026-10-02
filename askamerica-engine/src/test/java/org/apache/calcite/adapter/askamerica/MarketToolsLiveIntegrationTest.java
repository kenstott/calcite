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

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The prediction-market tools against the live Kalshi and Polymarket APIs: the listing reads
 * both venues, and one event of each can be read back with its rules and fee terms.
 */
@Tag("integration")
class MarketToolsLiveIntegrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test void bothVenuesListAndPrice() throws Exception {
    PredictionMarkets.Fetcher fetcher = new PredictionMarkets.HttpFetcher();
    MarketTools tools = new MarketTools(fetcher,
        new PredictionMarkets.ListingCache(fetcher, Duration.ofMinutes(15)),
        (q, limit) -> {
          throw new AssertionError("no query expected: " + q);
        }, Instant::now, Duration.ofMinutes(5).toMillis());

    ObjectNode args = MAPPER.createObjectNode();
    args.put("sample", 4);
    JsonNode out = MAPPER.readTree(tools.findCandidates(args));
    assertFalse(out.has("status"), out.toString());
    assertTrue(out.get("listing").get("kalshi_markets_read").asInt() > 0);
    assertTrue(out.get("listing").get("polymarket_markets_read").asInt() > 0);
    assertTrue(out.get("matched_by_venue").get("kalshi").asInt() > 0, out.toString());
    assertTrue(out.get("matched_by_venue").get("polymarket").asInt() > 0, out.toString());

    Set<String> priced = new HashSet<>();
    for (JsonNode e : out.get("events")) {
      String source = e.get("source").asText();
      if (!priced.add(source)) {
        continue;
      }
      ObjectNode one = MAPPER.createObjectNode();
      one.put("source", source);
      one.put("event_id", e.get("event_id").asText());
      JsonNode p = MAPPER.readTree(tools.priceEvent(one));
      assertEquals(e.get("event_id").asText(), p.get("event_id").asText());
      assertTrue(p.get("forecast").isNull());
      assertFalse(p.get("rules").asText().isEmpty(), source + " event has no rules text");
      assertTrue(p.get("priced_markets").size() + p.get("markets_without_condition").size()
          > 0, p.toString());
      assertTrue(p.has("other_venue"));
    }
    assertEquals(Set.of("kalshi", "polymarket"), priced);

    JsonNode baskets = MAPPER.readTree(tools.findBaskets(MAPPER.createObjectNode()));
    assertTrue(baskets.get("baskets").asInt() > 0, baskets.toString());
  }
}
