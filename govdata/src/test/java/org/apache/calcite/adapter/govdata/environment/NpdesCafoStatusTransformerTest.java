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
package org.apache.calcite.adapter.govdata.environment;

import com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link NpdesCafoStatusTransformer}. */
@Tag("unit")
class NpdesCafoStatusTransformerTest {

  private static final String URL = "https://www.epa.gov/report.pdf";

  @Test void readsStateAndTribalRowsAndIgnoresHeadings() {
    ArrayNode rows =
        NpdesCafoStatusTransformer.parseReport(2017, URL,
            "State Region 1 2 3\n"
                + "Michigan 5 309 294 2009 None needed exp 4/15\n"
                + "Indian Country (R7) 7 9 4\n"
                + "Oregon 10 1,117 1,390\n"
                + "TOTALS 1,435 1,688\n");
    assertEquals(3, rows.size());
    assertEquals("MI", rows.get(0).get("state_abbr").asText());
    assertEquals(309, rows.get(0).get("total_cafos").asLong());
    assertEquals(294, rows.get(0).get("permitted_cafos").asLong());
    assertTrue(rows.get(1).get("state_abbr").isNull());
    assertEquals(1390, rows.get(2).get("permitted_cafos").asLong());
  }

  @Test void failsWhenRowsDoNotAddUpToPrintedTotals() {
    assertThrows(IllegalStateException.class, () ->
        NpdesCafoStatusTransformer.parseReport(2024, URL,
            "Michigan 5 292 278\nTOTALS 300 278\n"));
  }

  @Test void failsWhenTotalsAreMissing() {
    assertThrows(IllegalStateException.class, () ->
        NpdesCafoStatusTransformer.parseReport(2024, URL, "Michigan 5 292 278\n"));
  }

  @Test void takesReportYearAndUrlFromPageLinks() {
    Map<Integer, String> links =
        NpdesCafoStatusTransformer.reportLinks(
            "<a href=\"/files/a b.pdf\" class=\"x\">NPDES CAFO Rule Implementation Status - "
                + "National Summary, Endyear 2021 (pdf)</a>"
                + "<a href=\"/files/other.pdf\">Fact sheet</a>"
                + "<a href=\"https://www.epa.gov/c.pdf\">Summary, Endyear 2022</a>",
            "https://www.epa.gov/npdes/reports");
    assertEquals(2, links.size());
    assertEquals("https://www.epa.gov/files/a%20b.pdf", links.get(2021));
    assertEquals("https://www.epa.gov/c.pdf", links.get(2022));
  }
}
