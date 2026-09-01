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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the {@code year_short} companion and, just as importantly, that it stays opt-in.
 *
 * <p>Every entry of a combination becomes part of the raw cache key when {@code rawCache.keyVars}
 * is unset, which is the default. A companion injected unconditionally would therefore change the
 * cache key of every table carrying a year and discard those caches wholesale — a mass re-download
 * across every schema in exchange for a companion a handful of tables reference. The opt-in
 * assertion below is what keeps that from being reintroduced by someone who reasonably assumes an
 * injected companion is free.
 */
@Tag("unit")
public class YearShortCompanionTest {

  private static Map<String, DimensionConfig> yearRange(int start, int end) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year").type(DimensionType.YEAR_RANGE).start(start).end(end).build());
    return dims;
  }

  private static Map<String, String> comboFor(List<Map<String, String>> combos, String year) {
    for (Map<String, String> c : combos) {
      if (year.equals(c.get("year"))) {
        return c;
      }
    }
    return null;
  }

  @Test void injectsTheLastTwoDigitsWhenRequested() {
    List<Map<String, String>> combos = new DimensionIterator()
        .expand(yearRange(2007, 2022), Collections.singleton("year_short"));

    assertEquals("07", comboFor(combos, "2007").get("year_short"), "must zero-pad");
    assertEquals("22", comboFor(combos, "2022").get("year_short"));
  }

  /** The IRS migration files are named for the year pair they span, so both halves are needed. */
  @Test void injectsThePriorYearsDigitsForYearPairFilenames() {
    List<Map<String, String>> combos = new DimensionIterator()
        .expand(yearRange(2022, 2022), Collections.singleton("year_short"));
    Map<String, String> c = comboFor(combos, "2022");

    assertEquals("21", c.get("year_short_prev"));
    assertEquals("22", c.get("year_short"));
  }

  /** Crossing a century boundary is ordinary modular arithmetic, not a special case. */
  @Test void priorYearRollsUnderAcrossACenturyBoundary() {
    List<Map<String, String>> combos = new DimensionIterator()
        .expand(yearRange(2000, 2000), Collections.singleton("year_short"));
    Map<String, String> c = comboFor(combos, "2000");

    assertEquals("00", c.get("year_short"));
    assertEquals("99", c.get("year_short_prev"), "1999, not a negative remainder");
  }

  /**
   * The guard that matters: not requesting it must leave every combination untouched, so tables
   * that never mention the companion keep the cache keys they already have.
   */
  @Test void isAbsentWhenNotRequested() {
    for (Map<String, String> c : new DimensionIterator().expand(yearRange(2020, 2024))) {
      assertFalse(c.containsKey("year_short"),
          "injecting unrequested would rekey every year-dimensioned table's raw cache");
      assertFalse(c.containsKey("year_short_prev"));
    }
  }

  /** An empty request set is treated the same as none, rather than falling through to inject. */
  @Test void anEmptyRequestSetInjectsNothing() {
    List<Map<String, String>> combos = new DimensionIterator()
        .expand(yearRange(2020, 2021), Collections.<String>emptySet());
    for (Map<String, String> c : combos) {
      assertFalse(c.containsKey("year_short"));
    }
  }

  /** Requesting it must not disturb the combination count or the year values themselves. */
  @Test void addsOnlyTheCompanionAndChangesNothingElse() {
    List<Map<String, String>> without = new DimensionIterator().expand(yearRange(2018, 2022));
    List<Map<String, String>> with = new DimensionIterator()
        .expand(yearRange(2018, 2022), Collections.singleton("year_short"));

    assertEquals(without.size(), with.size());
    for (int i = 0; i < without.size(); i++) {
      assertEquals(without.get(i).get("year"), with.get(i).get("year"));
      assertTrue(with.get(i).containsKey("year_short"));
    }
  }
}
