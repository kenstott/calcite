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
package org.apache.calcite.adapter.govdata.transport;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class FhwaTvtStateMonthlyTransformerTest {

  private static String section(String label, String links) {
    return "<strong class=\"lead d-inline-block font-weight-bold\">" + label + "</strong>\n"
        + "<div><ul>" + links + "</ul></div>\n";
  }

  private static String link(String href, String text) {
    return "<li><a href=\"" + href + "\" title=\"t\">" + text + "</a></li>";
  }

  private static final String INDEX =
      section("2026", link("/p/sep-2026", "September") + link("/node/299511", "August&nbsp;"))
          + section("2025", link("/p/dec-2025", "<span>December</span>"))
          + section("Archive", link("/p/old", "Report Archive 2002-2020"));

  @Test void readsEveryListedMonthAndSkipsArchiveSections() {
    TreeMap<String, String> pages = FhwaTvtStateMonthlyTransformer.parseIndex(INDEX);
    assertEquals(3, pages.size());
    assertEquals("/p/sep-2026", pages.get("2026-09"));
    assertEquals("/node/299511", pages.get("2026-08"));
    assertEquals("/p/dec-2025", pages.get("2025-12"));
  }

  @Test void monthNewerThanTheIndexIsNotPublished() {
    TreeMap<String, String> pages = FhwaTvtStateMonthlyTransformer.parseIndex(INDEX);
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> FhwaTvtStateMonthlyTransformer.monthPage(pages, 2026, 10));
    assertTrue(e.getMessage().contains("HTTP 404"), e.getMessage());
  }

  @Test void missingOlderMonthFails() {
    TreeMap<String, String> pages = FhwaTvtStateMonthlyTransformer.parseIndex(INDEX);
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> FhwaTvtStateMonthlyTransformer.monthPage(pages, 2026, 7));
    assertTrue(!e.getMessage().contains("HTTP 404"), e.getMessage());
  }

  @Test void changedIndexLayoutFails() {
    assertThrows(IllegalStateException.class,
        () -> FhwaTvtStateMonthlyTransformer.parseIndex("<html><body>moved</body></html>"));
    assertThrows(IllegalStateException.class, () -> FhwaTvtStateMonthlyTransformer.parseIndex(
        section("2026", link("/p/x", "Latest report"))));
  }

  @Test void picksTheWorkbookNamedForTheMonth() {
    String page = "<a href=\"/sites/fhwa.dot.gov/files/26septvt.pdf\">pdf</a>"
        + "<a href=\"/sites/fhwa.dot.gov/files/26septvt.xlsx\">xlsx</a>";
    assertEquals("/sites/fhwa.dot.gov/files/26septvt.xlsx",
        FhwaTvtStateMonthlyTransformer.workbookUrl(page, 2026, 9));
    assertThrows(IllegalStateException.class,
        () -> FhwaTvtStateMonthlyTransformer.workbookUrl(page, 2026, 8));
  }
}
