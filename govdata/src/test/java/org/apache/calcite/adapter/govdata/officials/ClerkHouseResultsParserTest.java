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
package org.apache.calcite.adapter.govdata.officials;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests of {@link ClerkHouseResultsParser}. */
@Tag("unit")
class ClerkHouseResultsParserTest {

  private static final ClerkHouseResultsParser.Release REL = ClerkHouseResultsParser.RELEASES[0];

  @Test void unknownPartyLabelFailsTheFetch() {
    String text = "ALABAMA\nFOR UNITED STATES REPRESENTATIVE\n"
        + "1. Jane Doe, Moonbase Party ........ 1,000\n";
    java.io.IOException e = assertThrows(java.io.IOException.class,
        () -> ClerkHouseResultsParser.parse(text, REL));
    assertTrue(e.getMessage().contains("neither a candidate with a known party label"),
        e.getMessage());
  }

  @Test void wrongSeatCountFailsTheFetch() {
    String text = "ALABAMA\nFOR UNITED STATES REPRESENTATIVE\n"
        + "1. Jane Doe, Republican ........ 1,000\nJohn Roe, Democrat ........ 900\n";
    java.io.IOException e = assertThrows(java.io.IOException.class,
        () -> ClerkHouseResultsParser.parse(text, REL));
    assertTrue(e.getMessage().contains("has districts [1], expected"), e.getMessage());
  }

  @Tag("integration")
  @Test void livePdfGivesThe2024ResultsWithTheKnownSeatSplitAndSpotValues() throws Exception {
    byte[] pdf;
    try (InputStream in = URI.create(REL.url).toURL().openStream()) {
      pdf = in.readAllBytes();
    }
    List<Map<String, Object>> rows = ClerkHouseResultsParser.parsePdf(pdf, REL);
    assertEquals(1279, rows.size());
    long aoc = 0;
    long golden = 0;
    for (Map<String, Object> r : rows) {
      if ("Alexandria Ocasio-Cortez".equals(r.get("candidate"))) {
        aoc += ((Long) r.get("candidate_votes")).longValue();
      }
      if ("ME".equals(r.get("state_postal")) && Integer.valueOf(2).equals(r.get("district"))
          && "Jared F. Golden".equals(r.get("candidate"))) {
        golden = ((Long) r.get("candidate_votes")).longValue();
        assertEquals(Long.valueOf(391596), r.get("total_votes"));
      }
    }
    assertEquals(118477L + 14237L, aoc);
    assertEquals(197151L, golden);
  }
}
