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
// storage-provider-guard:ignore-file - audited: only local temp files holding the downloaded workbook.
package org.apache.calcite.adapter.govdata.econ;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class CardDelinquencyByScoreTransformerTest {

  private static String table(String heading, String thirdHeader, String... values) {
    StringBuilder row = new StringBuilder("<tr><th class=\"stub\">9/30/2025</th>");
    for (String v : values) {
      row.append("<td class=\"data\">").append(v).append("</td>");
    }
    return "<div class=\"data-table\"><p><span class=\"tablesubhead\">" + heading
        + "</span><br /></p><table class=\"pubtables\"><thead><tr><th>Date</th>"
        + "<th>Subprime Credit Card Delinquency Rate</th>"
        + "<th>Near Prime Credit Card Delinquency Rate</th><th>" + thirdHeader
        + "</th></tr></thead><tbody>" + row + "</tr></tbody></table></div>";
  }

  private static String page(String thirdHeader, String... values) {
    // a look-alike table comes first and must not be read
    return "<html><body>"
        + table("Figure 1. Credit Card and Auto Loan Delinquency Rates", thirdHeader,
            "1", "2", "3")
        + table("Figure 5a. Credit Card Delinquency Rates", thirdHeader, values)
        + "</body></html>";
  }

  // the published header carries this misspelling
  private static final String PRIME = "Prime Credit Card Delnqinquency Rate";

  private static String transform(String html) {
    return new CardDelinquencyByScoreTransformer().transform(html, null);
  }

  @Test void emitsOneRowPerBandFromFigure5a() throws Exception {
    JsonNode rows = new ObjectMapper().readTree(transform(page(PRIME, "16.28", "5.67", "1.28")));
    assertEquals(3, rows.size());
    assertEquals("subprime", rows.get(0).get("score_band").asText());
    assertTrue(rows.get(0).get("score_min").isNull());
    assertEquals(619, rows.get(0).get("score_max").asInt());
    assertEquals("2025Q3", rows.get(0).get("period").asText());
    assertEquals(16.28, rows.get(0).get("pct_balance_30plus_delinquent").asDouble(), 1e-9);
    assertEquals("near_prime", rows.get(1).get("score_band").asText());
    assertEquals(5.67, rows.get(1).get("pct_balance_30plus_delinquent").asDouble(), 1e-9);
    assertEquals(720, rows.get(2).get("score_min").asInt());
    assertTrue(rows.get(2).get("score_max").isNull());
    assertEquals(1.28, rows.get(2).get("pct_balance_30plus_delinquent").asDouble(), 1e-9);
  }

  @Test void rejectsChangedShape() {
    assertThrows(IllegalStateException.class, () -> transform("<html><body/></html>"));
    assertThrows(IllegalStateException.class,
        () -> transform(page("Superprime Rate", "16.28", "5.67", "1.28")));
    assertThrows(IllegalStateException.class,
        () -> transform(page(PRIME, "16.28", "5.67")));
    assertThrows(NumberFormatException.class,
        () -> transform(page(PRIME, "16.28", "n.a.", "1.28")));
  }
}
