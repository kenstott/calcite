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
}
