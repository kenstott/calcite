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
package org.apache.calcite.adapter.govdata.officials;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the strict parsing of the MIT Election Lab presidential returns. */
@Tag("unit")
class PresidentialStateReturnsProviderTest {

  private static final String HEADER = "year,state,state_po,state_fips,state_cen,state_ic,office,"
      + "candidate,party_detailed,writein,candidatevotes,totalvotes,version,notes,"
      + "party_simplified\n";

  private static List<Map<String, Object>> parse(String body) throws IOException {
    return PresidentialStateReturnsProvider.parse(
        new BufferedReader(new StringReader(HEADER + body)), "rel");
  }

  @Test void rowsParseIncludingPublishedBlanks() throws Exception {
    List<Map<String, Object>> rows = parse(
        "1976,ALABAMA,AL,1,63,41.0,US PRESIDENT,\"CARTER, JIMMY\",DEMOCRAT,False,659170,"
        + "1182850,20210113,,DEMOCRAT\n"
        + "2024,ALABAMA,AL,1,63,41.0,US PRESIDENT,WRITE-IN,,True,8738,2265090,2025-11-20,,\n"
        + "2020,MARYLAND,MD,24,52,52.0,US PRESIDENT,,,,18045,3037030,20210113,,OTHER\n");
    assertEquals(3, rows.size());
    assertEquals("D", rows.get(0).get("party_major"));
    assertEquals("2021-01-13", rows.get(0).get("version_date"));
    assertEquals(1, rows.get(0).get("line_no"));
    assertEquals("write_in_aggregate", rows.get(1).get("candidate_kind"));
    assertEquals("2025-11-20", rows.get(1).get("version_date"));
    assertNull(rows.get(1).get("party_major"));
    assertEquals("unlabeled", rows.get(2).get("candidate_kind"));
    assertNull(rows.get(2).get("writein"));
  }

  @Test void nonPresidentOfficeFails() {
    assertThrows(IOException.class, () -> parse(
        "1976,ALABAMA,AL,1,63,41.0,US SENATE,X,DEMOCRAT,False,1,2,20210113,,DEMOCRAT\n"));
  }
}
