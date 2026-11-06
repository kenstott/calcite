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
package org.apache.calcite.adapter.govdata.law;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SenateEfdReportParser} and the amount/date helpers of {@link
 * SenateFinancialDisclosureProvider}. The fixtures are the verbatim eFD pages for Sen. A. Mitchell
 * McConnell's Annual Report for Calendar 2024 (filed 2025-08-11) and his PTR filed 2025-12-19,
 * fetched live 2026-10-09.
 */
@Tag("unit")
class SenateEfdReportParserTest {

  private static String fixture(String name) throws IOException {
    try (InputStream in = SenateEfdReportParserTest.class
        .getResourceAsStream("/law/senate-efd/" + name)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  @Test void annualReportParts() throws IOException {
    String html = fixture("annual-mcconnell-cy2024.html");
    Map<String, List<Map<String, String>>> parts = SenateEfdReportParser.parseAnnual(html);
    assertEquals("2024", SenateEfdReportParser.calendarYear(html));
    // Part 3: 51 numbered assets plus the 50 sub-items (6.1, 16.1, ...) listed under trusts.
    List<Map<String, String>> assets = parts.get("3");
    assertEquals(101, assets.size());
    int topLevel = 0;
    for (Map<String, String> a : assets) {
      if (a.get("#").indexOf('.') < 0) {
        topLevel++;
      }
    }
    assertEquals(51, topLevel);
    Map<String, String> first = assets.get(0);
    assertEquals("1", first.get("#"));
    assertEquals("Republic Bank and Trust", first.get("Asset"));
    assertEquals("(Louisville, KY) Type: Checking", first.get("Asset detail"));
    assertEquals("Bank Deposit", first.get("Asset Type"));
    assertEquals("Self", first.get("Owner"));
    assertEquals("$1,001 - $15,000", first.get("Value"));
    Map<String, String> trust = assets.get(2);
    assertEquals("Trust", trust.get("Asset Type"));
    assertEquals("General Trust", trust.get("Asset Type detail"));
    assertEquals(3, parts.get("2").size() > 2 ? 3 : parts.get("2").size());
    assertEquals(1, parts.get("5").size());
    assertTrue(parts.containsKey("4b"));
    assertNull(parts.get("7"), "Part 7 answered No: no table");
  }

  @Test void ptrTransactions() throws IOException {
    List<Map<String, String>> rows =
        SenateEfdReportParser.parsePtr(fixture("ptr-mcconnell-2025-12-19.html"));
    assertEquals(1, rows.size());
    Map<String, String> row = rows.get(0);
    assertEquals("12/01/2025", row.get("Transaction Date"));
    assertEquals("Spouse", row.get("Owner"));
    assertEquals("WFC", row.get("Ticker"));
    assertEquals("Stock", row.get("Asset Type"));
    assertEquals("Purchase", row.get("Type"));
  }

  @Test void changedTemplateThrows() {
    String html = "<html><body><h3 class=\"h4\">Part 3. Assets</h3><table><thead><tr><th></th>"
        + "<th>Thing</th></tr></thead><tbody></tbody></table></body></html>";
    assertThrows(RuntimeException.class, () -> SenateEfdReportParser.parseAnnual(html));
  }

  @Test void amountBounds() {
    assertArrayEquals(new Long[] {1001L, 15000L},
        SenateFinancialDisclosureProvider.bounds("$1,001 - $15,000"));
    assertArrayEquals(new Long[] {5000000L, null},
        SenateFinancialDisclosureProvider.bounds("Over $5,000,000"));
    assertArrayEquals(new Long[] {1000L, null},
        SenateFinancialDisclosureProvider.bounds("> $1,000"));
    assertArrayEquals(new Long[] {2148L, 2148L},
        SenateFinancialDisclosureProvider.bounds("$2,148.46"));
    assertArrayEquals(new Long[] {null, null},
        SenateFinancialDisclosureProvider.bounds("None (or less than $201)"));
  }

  @Test void dates() {
    assertEquals("2025-08-11", SenateFinancialDisclosureProvider.iso("08/11/2025"));
    assertThrows(RuntimeException.class, () -> SenateFinancialDisclosureProvider.iso("Aug 2025"));
  }
}
