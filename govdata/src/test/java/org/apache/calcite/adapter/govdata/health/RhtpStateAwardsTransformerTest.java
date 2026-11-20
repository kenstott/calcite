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
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link RhtpStateAwardsTransformer}. */
@Tag("unit")
class RhtpStateAwardsTransformerTest {

  private static String page(int states) {
    String[] names = {"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
        "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois",
        "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
        "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
        "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania",
        "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
        "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"};
    StringBuilder sb = new StringBuilder("<table><tr><th>State</th><th>FY26 Award Amount</th></tr>");
    for (int i = 0; i < states; i++) {
      sb.append("<tr><td>").append(names[i]).append("</td><td>$")
          .append(String.format("%,d", 147250806L + i)).append(" </td></tr>");
    }
    return sb.append("</table>").toString();
  }

  @Test void parsesFiftyStates() {
    List<Map<String, Object>> rows =
        RhtpStateAwardsTransformer.parse(page(50), 2026, "u", "2025-12-29");
    assertEquals(50, rows.size());
    assertEquals("AL", rows.get(0).get("state_abbr"));
    assertEquals(147250806L, rows.get(0).get("award_amount"));
    assertEquals(2026, rows.get(0).get("fiscal_year"));
  }

  @Test void rejectsWrongStateCount() {
    assertThrows(IllegalStateException.class,
        () -> RhtpStateAwardsTransformer.parse(page(49), 2026, "u", "2025-12-29"));
  }

  @Test void rejectsMissingTable() {
    assertThrows(IllegalStateException.class,
        () -> RhtpStateAwardsTransformer.parse("<p>none</p>", 2026, "u", "2025-12-29"));
  }
}
