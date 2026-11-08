/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces the reported bug: "campaign contributions by state" never surfaced any
 * fec.* table within a normal result window (rank ~140+ out of a limit=200 probe),
 * because the stopword "by" and the common column name "state" — reused verbatim on
 * dozens of unrelated tables across the catalog — scored as high as fec's own
 * genuinely on-topic name/comment match on "contributions", and there were enough of
 * them ahead of fec in schema iteration order to bury it.
 *
 * <p>The fix (stopword exclusion + a lower match weight for column names than for
 * schema/table names) brings fec up to the double digits — still tied with other
 * tables that are legitimately about "state" too (e.g. geo.state_ref), which is
 * correct: this only asserts fec is no longer effectively invisible.
 */
class CatalogSearchTest {

  @Test void campaignContributionsSurfacesAFecTableWellWithinTheDefaultLimit() {
    ArrayNode hits = Catalog.search("campaign contributions by state", 200);
    assertTrue(hits.size() > 0, "expected some matches");

    int fecRank = -1;
    for (int i = 0; i < hits.size(); i++) {
      ObjectNode h = (ObjectNode) hits.get(i);
      if ("fec".equals(h.path("schema").asText())) {
        fecRank = i;
        break;
      }
    }
    assertTrue(fecRank >= 0, "no fec.* table matched \"campaign contributions by state\" at all");
    // search_catalog's default limit is 40 — a caller who doesn't raise it must still see fec.
    assertTrue(fecRank < 20,
        "a genuinely on-topic fec table should rank well inside the default result "
            + "window, was at position " + fecRank + ": " + hits);
  }

  @Test void stopwordAloneMatchesNothing() {
    ArrayNode hits = Catalog.search("by", 50);
    assertTrue(hits.size() == 0,
        "a bare stopword should not incidentally match dozens of unrelated tables: " + hits);
  }

  /** Index of the first hit on schema.table, or -1. */
  private static int rankOf(ArrayNode hits, String schemaTable) {
    for (int i = 0; i < hits.size(); i++) {
      if ((hits.get(i).path("schema").asText() + "." + hits.get(i).path("table").asText())
          .equals(schemaTable)) {
        return i;
      }
    }
    return -1;
  }

  /** BM25: a table whose description matches every word of the query ranks near the top even
   *  when other tables carry one of the words in their NAME. The substring scorer this
   *  replaced never surfaced econ.regional_employment for this query. */
  @Test void descriptionMatchingTheWholeQueryRanksHigh() {
    int r = rankOf(Catalog.search("unemployment rate state monthly", 50),
        "econ.regional_employment");
    assertTrue(r >= 0 && r < 10, "econ.regional_employment ranked " + r);
  }

  /** IDF: a word found all over the catalog cannot outweigh a rare one. */
  @Test void rareWordOutweighsCommonWords() {
    int r = rankOf(Catalog.search("rent burden rate", 50), "census.acs_congressional_district");
    assertTrue(r >= 0 && r < 10, "census.acs_congressional_district ranked " + r);
  }

  @Test void tokensStemPluralsAndDropStopwords() {
    org.junit.jupiter.api.Assertions.assertEquals(
        java.util.Arrays.asList("county", "rate", "renter", "household", "census"),
        Catalog.tokens("Counties rates of the renters_households census"));
  }

  /** A code listed in a column comment ("Codes: ICSA = initial jobless claims ...") is found
   *  by its meaning and named with the filter that selects it. */
  @Test void listedCodeIsFoundByMeaning() {
    ArrayNode hits = Catalog.search("initial jobless claims", 20);
    boolean icsa = false;
    for (int i = 0; i < Math.min(3, hits.size()); i++) {
      if ("ICSA".equals(hits.get(i).path("code").asText())
          && "fred_indicators".equals(hits.get(i).path("table").asText())) {
        icsa = true;
      }
    }
    assertTrue(icsa, "fred_indicators series code ICSA not in the top 3: " + hits);
  }

  @Test void codesSectionParses() {
    java.util.List<String[]> codes = Catalog.codesIn(
        "Series id. Codes: ICSA = initial jobless claims, weekly; UNRATE = unemployment rate.");
    org.junit.jupiter.api.Assertions.assertEquals(2, codes.size());
    org.junit.jupiter.api.Assertions.assertEquals("ICSA", codes.get(0)[0]);
    org.junit.jupiter.api.Assertions.assertEquals("unemployment rate", codes.get(1)[1]);
    org.junit.jupiter.api.Assertions.assertEquals("Series id.",
        Catalog.withoutCodes("Series id. Codes: ICSA = x"));
  }
}
