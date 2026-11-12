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
class SocialSecurityTrusteesTransformerTest {

  private static String cells(String label, String... values) {
    StringBuilder sb = new StringBuilder("<tr><td colspan=\"2\"><div><a name=\"1\">" + label
        + "\t</a></div></td>");
    for (String v : values) {
      sb.append("<td><div><a name=\"2\">").append(v).append("</a></div></td>");
    }
    return sb.append("</tr>\n").toString();
  }

  private static final String OASDI_NEW = "<p>After reserves for the OASDI program are depleted, "
      + "continuing income is sufficient to pay 83&nbsp;percent of OASDI scheduled benefits for "
      + "the rest of 2034, declining to 65&nbsp;percent for 2100.</p>";
  private static final String OASDI_OLD = "<p>After trust fund reserve depletion, continuing "
      + "income is sufficient to support expenditures at a level of 79 percent of program cost "
      + "for the rest of 2034, declining to 74 percent for 2090.</p>";
  private static final String OASI_NEW = "<p>After the reserves are depleted, projected OASI income "
      + "is sufficient to pay 78 percent of scheduled OASI benefits for the rest of 2032, "
      + "declining to 62 percent for 2100.</p>";

  private static String balance(String oasi, String di, String oasdi) {
    return "<table><tr><td>Shown in last year's report:</td></tr>"
        + cells("Actuarial balance", "-3.95", ".12", "-3.82")
        + "<tr><td>Shown in this report:</td></tr>"
        + cells("Actuarial balance", oasi, di, oasdi);
  }

  private static String page(String depletion) {
    return page(depletion, OASDI_NEW + OASI_NEW);
  }

  private static String page(String depletion, String payable) {
    return payable + balance("-4.55", ".13", "-4.42")
        + "</table><p>Through the end of 2100, the combined funds have a present-value unfunded "
        + "obligation of $29.3&nbsp;trillion. If</p><table>"
        + "<tr><th>OASI</th><th>DI</th><th>OASDI</th></tr>"
        + cells("Maximum projected trust fund ratio (percent)", "153", "857", "151")
        + "<tr><td>&nbsp;</td></tr>"
        + cells("Year attained", "2026", "2100", "2026")
        + depletion + "</table><p>a footnote</p>";
  }

  @Test void parsesDepletionAndFootnoteMarker() {
    String html = page(cells("Projected year of trust fund reserve depletion ", "2032",
        "<sup>a</sup>", "2034") + "<tr><td><sup>a</sup> The reserves remain positive</td></tr>");
    List<Map<String, Object>> rows = SocialSecurityTrusteesTransformer.parse(2026, html);
    assertEquals(3, rows.size());
    assertEquals("OASI", rows.get(0).get("fund"));
    assertEquals(2032, rows.get(0).get("depletion_year"));
    assertEquals(153, rows.get(0).get("max_trust_fund_ratio_pct"));
    assertEquals(2026, rows.get(0).get("max_ratio_year"));
    assertNull(rows.get(0).get("unfunded_obligation_trillions"));
    assertEquals("DI", rows.get(1).get("fund"));
    assertNull(rows.get(1).get("depletion_year"));
    assertEquals(857, rows.get(1).get("max_trust_fund_ratio_pct"));
    assertEquals(2100, rows.get(1).get("max_ratio_year"));
    assertEquals("OASDI", rows.get(2).get("fund"));
    assertEquals(2034, rows.get(2).get("depletion_year"));
    assertEquals(29.3, rows.get(2).get("unfunded_obligation_trillions"));
    assertEquals(2026, rows.get(2).get("report_year"));
  }

  @Test void parsesActuarialBalanceAndPayableShare() {
    String html = page(cells("Projected year of trust fund reserve depletion ", "2032",
        "<sup>a</sup>", "2034") + "<tr><td>x</td></tr>");
    List<Map<String, Object>> rows = SocialSecurityTrusteesTransformer.parse(2026, html);
    assertEquals(-4.55, rows.get(0).get("actuarial_balance_pct_payroll"));
    assertEquals(0.13, rows.get(1).get("actuarial_balance_pct_payroll"));
    assertEquals(-4.42, rows.get(2).get("actuarial_balance_pct_payroll"));
    assertEquals(78, rows.get(0).get("payable_benefits_pct_at_depletion"));
    assertEquals(62, rows.get(0).get("payable_benefits_pct_final_year"));
    assertEquals(2100, rows.get(0).get("payable_projection_end_year"));
    assertNull(rows.get(1).get("payable_benefits_pct_at_depletion"));
    assertEquals(83, rows.get(2).get("payable_benefits_pct_at_depletion"));
    assertEquals(65, rows.get(2).get("payable_benefits_pct_final_year"));
  }

  @Test void parsesOlderReportWordingWithoutOasiSentence() {
    String html = page(cells("Projected year of trust fund reserve depletion ", "2035", "2023",
        "2034") + "<tr><td>x</td></tr>", OASDI_OLD);
    List<Map<String, Object>> rows = SocialSecurityTrusteesTransformer.parse(2016, html);
    assertNull(rows.get(0).get("payable_benefits_pct_at_depletion"));
    assertEquals(79, rows.get(2).get("payable_benefits_pct_at_depletion"));
    assertEquals(74, rows.get(2).get("payable_benefits_pct_final_year"));
    assertEquals(2090, rows.get(2).get("payable_projection_end_year"));
  }

  @Test void payableSentenceForOtherYearFails() {
    String html = page(cells("Projected year of trust fund reserve depletion ", "2032",
        "<sup>a</sup>", "2035") + "<tr><td>x</td></tr>");
    assertThrows(IllegalStateException.class,
        () -> SocialSecurityTrusteesTransformer.parse(2026, html));
  }

  @Test void parsesNumericDiDepletion() {
    String html = page(cells("Projected year of trust fund reserve depletion ", "2032", "2023",
        "2034") + "<tr><td>next</td></tr>");
    List<Map<String, Object>> rows = SocialSecurityTrusteesTransformer.parse(2016, html);
    assertEquals(2023, rows.get(1).get("depletion_year"));
  }

  @Test void changedLayoutFails() {
    assertThrows(IllegalStateException.class,
        () -> SocialSecurityTrusteesTransformer.parse(2026, "<html>layout changed</html>"));
  }
}
