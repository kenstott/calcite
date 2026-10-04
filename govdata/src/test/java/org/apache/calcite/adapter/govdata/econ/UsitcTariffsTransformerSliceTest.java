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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The split-and-plan loop of {@link UsitcTariffsTransformer}, with DataWeb replaced by a fake that
 * reports any slice wider than {@code maxHeadings} as never finishing — the case that costs a full
 * query deadline of dead waiting in production.
 */
@Tag("unit")
class UsitcTariffsTransformerSliceTest {

  private static final Map<String, String> NO_HEADERS = new LinkedHashMap<String, String>();

  /** Fake DataWeb: a slice is too slow past {@code maxHeadings} headings; otherwise it returns one
   *  row per heading it was asked for. */
  private static final class FakeDataWeb extends UsitcTariffsTransformer {
    final int maxHeadings;
    final List<String> requested = new ArrayList<String>();
    int slow;

    FakeDataWeb(int maxHeadings) {
      this.maxHeadings = maxHeadings;
    }

    @Override int fetchOnce(String url, Map<String, String> headers, String year, String label,
        String codes, Map<String, Object[]> merged) throws IOException {
      requested.add(codes);
      // A bare chapter code ("84") stands for all 100 of its headings.
      int headings = codes.indexOf(',') < 0 && codes.length() == 4 ? 100 : codes.split(",").length;
      if (headings > maxHeadings) {
        slow++;
        throw new QueryTooSlowException("was still unfinished after 8 polls over 120s");
      }
      if (codes.length() == 4) {
        // bare chapter ("84" quoted is 4 chars): every heading
        String chapter = codes.substring(1, 3);
        for (int i = 0; i < 100; i++) {
          merged.put(chapter + String.format("%02d", i), new Object[] {chapter});
        }
      } else {
        for (String code : codes.split(",")) {
          merged.put(code.replace("\"", ""), new Object[] {code});
        }
      }
      return headings;
    }
  }

  private static Set<String> keys(Map<String, Object[]> merged) {
    return new TreeSet<String>(merged.keySet());
  }

  @Test void aColdRunSplitsReactivelyAndRecordsEveryCut() throws Exception {
    FakeDataWeb web = new FakeDataWeb(25);
    UsitcSlicePlan learned = UsitcSlicePlan.empty();
    Map<String, Object[]> merged = new LinkedHashMap<String, Object[]>();

    web.fetchChapter("u", NO_HEADERS, "2025", "84", UsitcSlicePlan.empty(), learned, merged);

    // 100 -> 50+50 -> 25+25+25+25: three splits, three cuts, and three timed-out queries.
    assertEquals(3, web.slow);
    assertEquals(4, learned.slices("84").size());
    assertEquals(100, merged.size());
  }

  @Test void aPlannedRunStartsFromTheLearnedSlicesAndWastesNoDeadline() throws Exception {
    FakeDataWeb cold = new FakeDataWeb(25);
    UsitcSlicePlan learned = UsitcSlicePlan.empty();
    Map<String, Object[]> coldRows = new LinkedHashMap<String, Object[]>();
    cold.fetchChapter("u", NO_HEADERS, "2025", "84", UsitcSlicePlan.empty(), learned, coldRows);

    FakeDataWeb warm = new FakeDataWeb(25);
    UsitcSlicePlan relearned = UsitcSlicePlan.empty();
    Map<String, Object[]> warmRows = new LinkedHashMap<String, Object[]>();
    warm.fetchChapter("u", NO_HEADERS, "2025", "84", learned, relearned, warmRows);

    assertEquals(0, warm.slow, "no query may be sent that is known to time out");
    assertEquals(4, warm.requested.size());
    assertFalse(warm.requested.contains("\"84\""), "the bare chapter must not be requested");
    assertTrue(relearned.isEmpty(), "nothing new was learned");
    assertEquals(keys(coldRows), keys(warmRows), "a plan must not change which rows come back");
  }

  @Test void aChapterNoRunHadToSplitIsStillOneRequest() throws Exception {
    FakeDataWeb web = new FakeDataWeb(100);
    UsitcSlicePlan planned = UsitcSlicePlan.empty();
    planned.addCut("84", 50);
    Map<String, Object[]> merged = new LinkedHashMap<String, Object[]>();

    web.fetchChapter("u", NO_HEADERS, "2025", "01", planned, UsitcSlicePlan.empty(), merged);

    assertEquals(1, web.requested.size());
    assertEquals("\"01\"", web.requested.get(0));
    assertEquals(100, merged.size());
  }

  @Test void aPlannedSliceThatHasSinceGrownHeavySplitsAndRecordsTheNewCuts() throws Exception {
    FakeDataWeb web = new FakeDataWeb(25);
    UsitcSlicePlan planned = UsitcSlicePlan.empty();
    planned.addCut("84", 50);   // learned when DataWeb could still do 50 headings at once
    UsitcSlicePlan learned = UsitcSlicePlan.empty();
    Map<String, Object[]> merged = new LinkedHashMap<String, Object[]>();

    web.fetchChapter("u", NO_HEADERS, "2025", "84", planned, learned, merged);

    assertEquals(2, web.slow, "each stale 50-heading slice times out once, then splits");
    assertEquals(100, merged.size());
    assertEquals(3, learned.slices("84").size(), "cuts 25 and 75 were learned");
    assertEquals(25, learned.slices("84").get(1)[0]);
    assertEquals(75, learned.slices("84").get(2)[0]);
  }

  @Test void aSliceThatCannotBeSplitFurtherStillFailsLoud() {
    FakeDataWeb web = new FakeDataWeb(0);
    Map<String, Object[]> merged = new LinkedHashMap<String, Object[]>();
    IOException e = assertThrows(IOException.class, () -> web.fetchChapter(
        "u", NO_HEADERS, "2025", "84", UsitcSlicePlan.empty(), UsitcSlicePlan.empty(), merged));
    assertTrue(e.getMessage().contains("cannot be split further"), e.getMessage());
  }
}
