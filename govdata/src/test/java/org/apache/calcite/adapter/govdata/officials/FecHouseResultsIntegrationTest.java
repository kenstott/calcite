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

import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Parses the real FEC 2020 and 2022 workbooks (downloaded from fec.gov) and checks the cycle
 * totals. Needs network access.
 */
@Tag("integration")
class FecHouseResultsIntegrationTest {

  private static byte[] download(String url) throws Exception {
    try (InputStream in = URI.create(url).toURL().openStream()) {
      return in.readAllBytes();
    }
  }

  @Test void fecCyclesHave435DecisiveDistrictsAndTheKnownSeatSplit() throws Exception {
    Map<Integer, int[]> expected = new HashMap<Integer, int[]>();
    expected.put(2020, new int[] {222, 213});
    expected.put(2022, new int[] {213, 222});
    for (FecHouseResultsParser.Release rel : FecHouseResultsParser.RELEASES) {
      List<Map<String, Object>> rows = FecHouseResultsParser.parseWorkbook(download(rel.url), rel);
      // Winner by district for the deciding non-special round: summed per candidate name.
      Map<String, Map<String, Long>> votes = new HashMap<String, Map<String, Long>>();
      Map<String, Map<String, String>> party = new HashMap<String, Map<String, String>>();
      Set<String> hasRunoff = new HashSet<String>();
      for (Map<String, Object> r : rows) {
        if (((Boolean) r.get("special")).booleanValue()
            || r.get("candidate") == null
            || ((Boolean) r.get("writein")).booleanValue()) {
          continue;
        }
        String d = r.get("state_postal") + "-" + r.get("district");
        if (((Boolean) r.get("runoff")).booleanValue()) {
          hasRunoff.add(d);
        }
      }
      for (Map<String, Object> r : rows) {
        if (((Boolean) r.get("special")).booleanValue() || r.get("candidate") == null) {
          continue;
        }
        String d = r.get("state_postal") + "-" + r.get("district");
        if (hasRunoff.contains(d) != ((Boolean) r.get("runoff")).booleanValue()) {
          continue;
        }
        String c = (String) r.get("candidate");
        Map<String, Long> m = votes.get(d);
        if (m == null) {
          m = new HashMap<String, Long>();
          votes.put(d, m);
          party.put(d, new HashMap<String, String>());
        }
        m.merge(c, (Long) r.get("candidate_votes"), Long::sum);
        String pm = (String) r.get("party_major");
        if ("D".equals(pm) || "R".equals(pm)) {
          party.get(d).put(c, pm);
        }
      }
      assertEquals(435, votes.size(), "districts " + rel.year);
      Map<String, Integer> seats = new TreeMap<String, Integer>();
      for (Map.Entry<String, Map<String, Long>> e : votes.entrySet()) {
        String win = null;
        long best = -1;
        for (Map.Entry<String, Long> c : e.getValue().entrySet()) {
          if (c.getValue().longValue() > best) {
            best = c.getValue().longValue();
            win = c.getKey();
          }
        }
        String p = party.get(e.getKey()).get(win);
        seats.merge(p == null ? "other" : p, 1, Integer::sum);
      }
      assertEquals(expected.get(rel.year)[0], seats.get("D").intValue(), "D " + rel.year);
      assertEquals(expected.get(rel.year)[1], seats.get("R").intValue(), "R " + rel.year);
    }
  }
}
