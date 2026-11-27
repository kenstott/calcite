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
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class SocialSecurityRetirementApplicationsTransformerTest {

  private static final String HEADER = "Fiscal Year (Field A),Month  (Field B),Total (Field C),"
      + "Internet (Field D),% (Field E),Comments (Field F)\n";

  @Test void mapsFiscalYearToCalendarYearAndParsesQuotedNumbers() {
    String csv = HEADER + "2025,Oct,\"272,055\",\"161,577\",59.39,\n"
        + "2025,Jan,\"409,867\",\"278,271\",67.89,\n";
    List<Map<String, Object>> rows = SocialSecurityRetirementApplicationsTransformer.parse(csv);
    assertEquals(2, rows.size());
    assertEquals(2024, rows.get(0).get("year"));
    assertEquals(10, rows.get(0).get("month"));
    assertEquals(2025, rows.get(0).get("fiscal_year"));
    assertEquals(272055L, rows.get(0).get("total_applications"));
    assertEquals(2025, rows.get(1).get("year"));
    assertEquals(67.89, rows.get(1).get("pct_internet"));
  }

  @Test void unknownMonthFails() {
    assertThrows(IllegalStateException.class, () ->
        SocialSecurityRetirementApplicationsTransformer.parse(HEADER + "2025,Foo,\"1\",\"1\",1,\n"));
  }

  @Test void emptyFileFails() {
    assertThrows(IllegalStateException.class,
        () -> SocialSecurityRetirementApplicationsTransformer.parse(HEADER));
  }
}
