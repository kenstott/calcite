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
class SocialSecurityColaTransformerTest {

  private static String row(String year, String cola) {
    return "<tr><td align=\"left\">" + year + "</td>\n <td align=\"right\">" + cola
        + "</td></tr>\n";
  }

  @Test void parsesRowsEffectiveMonthAndFootnote() {
    String html = "<table>" + row("1975", "8.0") + row("1983", "3.5")
        + row("1999<small><sup>&nbsp;a</sup></small>", "2.5") + row("2022", "8.7")
        + row("2026", "3.6") + "</table>"
        + "<small><sup>a</sup>\n The COLA for December 1999 was originally determined as 2.4\n"
        + "percent. Pursuant to Public Law 106-554, however, this COLA is effectively now "
        + "2.5 percent.</small>";
    List<Map<String, Object>> rows = SocialSecurityColaTransformer.parse(html);
    assertEquals(5, rows.size());
    assertEquals(1975, rows.get(0).get("year"));
    assertEquals(8.0, rows.get(0).get("cola_pct"));
    assertEquals(6, rows.get(0).get("effective_month"));
    assertEquals(12, rows.get(1).get("effective_month"));
    assertEquals(1999, rows.get(2).get("year"));
    assertEquals("The COLA for December 1999 was originally determined as 2.4 percent. "
        + "Pursuant to Public Law 106-554, however, this COLA is effectively now 2.5 percent.",
        rows.get(2).get("footnote"));
    assertNull(rows.get(3).get("footnote"));
    assertEquals(3.6, rows.get(4).get("cola_pct"));
  }

  @Test void pageWithoutRowsFails() {
    assertThrows(IllegalStateException.class,
        () -> SocialSecurityColaTransformer.parse("<html>layout changed</html>"));
  }
}
