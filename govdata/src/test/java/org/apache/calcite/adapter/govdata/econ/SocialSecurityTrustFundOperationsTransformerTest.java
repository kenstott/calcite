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
package org.apache.calcite.adapter.govdata.econ;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class SocialSecurityTrustFundOperationsTransformerTest {

  private static String row(String... cells) {
    StringBuilder sb = new StringBuilder("<tr align=\"right\">");
    for (int i = 0; i < cells.length; i++) {
      sb.append("<td").append(i == 0 ? " align=\"left\"" : "").append(">").append(cells[i])
          .append("</td>");
    }
    return sb.append("</tr>\n").toString();
  }

  private static String table(String... rows) {
    StringBuilder sb = new StringBuilder("<table><caption>x</caption><thead><tr><th>h</th></tr>"
        + "</thead>");
    for (String r : rows) {
      sb.append(r);
    }
    return sb.append("</table>").toString();
  }

  private static String page(String incomeRow2025) {
    return table(row("1957", "$8,090", "$7,567", "$523", "$23,042"),
        row("2025", "1,449,306", "1,596,519", "-160,160", "2,561,306"))
        + table(row("1957", "$8,090", "$7,000", "&hellip;", "&hellip;", "$1,000"),
            incomeRow2025)
        + table(row("1957", "$7,567", "$7,500", "$50", "&hellip;"),
            row("2025", "1,596,519", "1,596,519", "6,952", "-$1"));
  }

  @Test void joinsSummaryIncomeAndCostByYear() {
    List<Map<String, Object>> rows = SocialSecurityTrustFundOperationsTransformer.parse("OASDI",
        1957, page(row("2025", "1,449,306", "1,322,618", "57,764", "(c)", "68,924")));
    assertEquals(2, rows.size());
    Map<String, Object> r = rows.get(1);
    assertEquals("OASDI", r.get("fund"));
    assertEquals(2025, r.get("calendar_year"));
    assertEquals(1449306L, r.get("total_income"));
    assertEquals(1322618L, r.get("payroll_tax_income"));
    assertNull(r.get("general_fund_transfers"));
    assertEquals(1596519L, r.get("total_cost"));
    assertEquals(-1L, r.get("railroad_transfers"));
    assertEquals(-160160L, r.get("net_change"));
    assertEquals(2561306L, r.get("assets_end"));
    assertNull(rows.get(0).get("benefit_taxation_income"));
  }

  @Test void disagreeingTotalsFail() {
    assertThrows(IllegalStateException.class,
        () -> SocialSecurityTrustFundOperationsTransformer.parse("OASDI", 1957,
            page(row("2025", "9", "1", "1", "1", "1"))));
  }

  @Test void changedLayoutFails() {
    assertThrows(IllegalStateException.class,
        () -> SocialSecurityTrustFundOperationsTransformer.parse("OASDI", 1957, "<html/>"));
  }
}
