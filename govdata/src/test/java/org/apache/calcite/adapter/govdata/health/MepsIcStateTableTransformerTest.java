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
package org.apache.calcite.adapter.govdata.health;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class MepsIcStateTableTransformerTest {

  private static final String HEAD =
      "<thead><tr><th>Division and State </th><th>Total</th><th>Less than 10 employees</th>"
      + "<th>10-24 employees</th><th>25-99 employees</th><th>100-999 employees</th>"
      + "<th>1000 or more employees</th><th>Less than 50 employees</th>"
      + "<th>50 or more employees</th></tr></thead>";

  private static String row(String stub, String... cells) {
    StringBuilder sb = new StringBuilder("<tr><th>" + stub + " </th>");
    for (String c : cells) {
      sb.append("<td>").append(c).append("&nbsp;&nbsp;&nbsp;</td>");
    }
    return sb.append("</tr>").toString();
  }

  private static String page(String estUs, String estCt, String seUs, String seCt) {
    return "<html><body>"
        + "<table class=\"dataTable\"><caption>Table II.A.2 Percent</caption>" + HEAD + "<tbody>"
        + estUs + "<tr><th>New England: </th></tr>" + estCt + "</tbody></table>"
        + "<table class=\"dataTable\"><caption>Table II.A.2 Standard errors for percent</caption>"
        + HEAD + "<tbody>" + seUs + "<tr><th>New England: </th></tr>" + seCt
        + "</tbody></table></body></html>";
  }

  @Test void parsesEstimatesSuppressionAndReliability() {
    String html = page(
        row("United States", "51.1%", "22.3%", "52.7%", "76.7%", "96.4%", "98.9%", "31.0%",
            "96.7%"),
        row("Connecticut", "56.3%", "22.9%", "58.3%", "--", "98.9%*", "100.0%", "34.0%",
            "98.4%"),
        row("United States", "0.47%", "0.5%", "0.6%", "0.7%", "0.8%", "0.9%", "0.4%", "0.3%"),
        row("Connecticut", "1.1%", "1.2%", "1.3%", "--", "1.5%", "0.0%", "1.7%", "1.8%"));
    List<Map<String, Object>> rows = MepsIcStateTableTransformer.parse(html, 2020, "tiia2",
        "establishments_offering_pct", "percent", "u");
    assertEquals(16, rows.size());
    Map<String, Object> us = rows.get(0);
    assertEquals("national", us.get("geo_level"));
    assertNull(us.get("state_abbr"));
    assertEquals(51.1, (Double) us.get("estimate"), 1e-9);
    assertEquals(0.47, (Double) us.get("std_error"), 1e-9);
    Map<String, Object> ctSuppressed = rows.get(11);
    assertEquals("CT", ctSuppressed.get("state_abbr"));
    assertEquals("New England", ctSuppressed.get("census_division"));
    assertEquals("25-99 employees", ctSuppressed.get("firm_size"));
    assertNull(ctSuppressed.get("estimate"));
    Map<String, Object> ctLow = rows.get(12);
    assertEquals(98.9, (Double) ctLow.get("estimate"), 1e-9);
    assertTrue((Boolean) ctLow.get("low_reliability"));
  }

  @Test void parsesDollarsWithThousandsSeparator() {
    assertEquals(7123.0, MepsIcStateTableTransformer.cell("$7,123", "r", "u").value, 1e-9);
  }

  @Test void nonNumericCellFails() {
    assertThrows(IllegalStateException.class,
        () -> MepsIcStateTableTransformer.cell("n/a", "r", "u"));
  }

  @Test void missingStandardErrorTableFails() {
    String html = "<html><body><table class=\"dataTable\"><caption>x</caption>" + HEAD
        + "<tbody></tbody></table></body></html>";
    assertThrows(IllegalStateException.class,
        () -> MepsIcStateTableTransformer.parse(html, 2020, "tiia2", "m", "percent", "u"));
  }
}
