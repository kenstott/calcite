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
package org.apache.calcite.adapter.govdata.fiscal;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class SnapParticipantsByAgeProviderTest {

  private static String header() {
    StringBuilder sb = new StringBuilder("STATE,STATENAME,STATUS,FYWGT,YRMONTH");
    for (int k = 1; k <= 16; k++) {
      sb.append(",AGE").append(k).append(",FSAFIL").append(k);
    }
    return sb.toString();
  }

  /** Builds a row; members are {age, affil} pairs. */
  private static String row(String st, String name, int status, double wgt, int ym, int[]... members) {
    StringBuilder sb = new StringBuilder();
    sb.append(st).append(',').append(name).append(',').append(status).append(',').append(wgt)
        .append(',').append(ym);
    for (int k = 0; k < 16; k++) {
      if (k < members.length) {
        sb.append(',').append(members[k][0]).append(',').append(members[k][1]);
      } else {
        sb.append(",,");
      }
    }
    return sb.toString();
  }

  @Test void aggregatesAgeBandsForSnapMembersOnly() throws IOException {
    String csv = header() + "\n"
        + row("20", "Kansas", 1, 100, 202210, new int[]{3, 1}, new int[]{10, 1}, new int[]{35, 1},
            new int[]{70, 1}, new int[]{40, 4}) + "\n"
        + row("20", "Kansas", 2, 50, 202211, new int[]{17, 1}, new int[]{18, 1}) + "\n"
        + row("20", "Kansas", 4, 999, 202211, new int[]{5, 1}) + "\n";
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    new SnapParticipantsByAgeProvider().parseCsv(new BufferedReader(new StringReader(csv)), 2023, rows);
    assertEquals(1, rows.size());
    Map<String, Object> r = rows.get(0);
    assertEquals("20", r.get("state_fips"));
    assertEquals("KS", r.get("state_abbr"));
    assertEquals(2023, r.get("year"));
    assertEquals(150L, r.get("households_participating"));
    assertEquals(500L, r.get("persons_participating")); // 4*100 + 2*50; non-member and STATUS 4 row excluded
    assertEquals(100L, r.get("children_under_5"));
    assertEquals(150L, r.get("children_5_to_17"));
    assertEquals(250L, r.get("children_under_18"));
    assertEquals(150L, r.get("adults_18_to_59"));
    assertEquals(100L, r.get("adults_60_plus"));
    assertEquals(2L, r.get("qc_sample_households"));
    assertEquals(202210, r.get("qc_first_month"));
    assertEquals(202211, r.get("qc_last_month"));
  }

  @Test void stateCodeNameMismatchFails() {
    String csv = header() + "\n" + row("6", "Kansas", 1, 1, 202210, new int[]{3, 1}) + "\n";
    assertThrows(IOException.class, () -> new SnapParticipantsByAgeProvider()
        .parseCsv(new BufferedReader(new StringReader(csv)), 2023, new ArrayList<Map<String, Object>>()));
  }
}
