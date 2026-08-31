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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies publication lag expressed in months.
 *
 * <p>{@code dataMonthLag} expresses the same concern as {@code dataLag} at a finer granularity, but
 * the two are <em>not</em> interchangeable: a year lag shifts the axis while a month lag caps it.
 * The properties worth pinning are that difference, that declaring both is refused, and that the
 * year boundary needs no special handling — a lag crossing January should fall out of ordinary date
 * arithmetic rather than from bespoke logic.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(PipelineClockTest.CLOCK_LOCK)
public class DataMonthLagTest {

  @AfterEach void unpin() {
    PipelineClock.clearOverride();
  }

  private static Map<String, Object> map(Object... kv) {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(String.valueOf(kv[i]), kv[i + 1]);
    }
    return m;
  }

  /** The data years reached — {@code effective_year}, which is what partitions and URLs use. */
  private static List<String> dataYears(DimensionConfig year) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", year);
    List<String> out = new java.util.ArrayList<String>();
    for (Map<String, String> c : new DimensionIterator().expand(dims)) {
      out.add(c.containsKey("effective_year") ? c.get("effective_year") : c.get("year"));
    }
    return out;
  }

  private static List<String> years(DimensionConfig year) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", year);
    List<Map<String, String>> combos = new DimensionIterator().expand(dims);
    List<String> out = new java.util.ArrayList<String>();
    for (Map<String, String> c : combos) {
      out.add(c.get("year"));
    }
    return out;
  }

  private static DimensionConfig yearRange(Map<String, Object> extra) {
    Map<String, Object> m = map("name", "year", "type", "yearRange", "start", 2018, "end", "current");
    m.putAll(extra);
    return DimensionConfig.fromMap("year", m);
  }

  // --- how the two lags relate -----------------------------------------------------------------------

  /**
   * The two express publication lag but are NOT interchangeable, and the difference is worth
   * pinning because it is easy to assume otherwise.
   *
   * <p>{@code dataLag} <em>shifts the whole axis</em>: {@code year} stays the publish year and
   * {@code effective_year = year - lag} carries the data year, so both move together. A month lag
   * cannot do that — shifting a <em>year label</em> by two months is not meaningful, and applying
   * it per-combo would drag every historical year backwards. {@code dataMonthLag} therefore
   * <em>caps the ceiling</em> instead: the axis stops at the newest published data year and
   * {@code year} already is that year.
   *
   * <p>Both land the correct value where it is consumed — the partition and URL read
   * {@code effective_year} — but the top of the range differs, so an equivalent-looking pair of
   * values is not a drop-in substitution.
   */
  @Test void theTwoLagsDifferInHowTheyReachTheDataYear() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 8, 15));

    // dataLag: publish years run to the current year; effective_year trails by the lag.
    assertEquals("2026", years(yearRange(map("dataLag", 2))).get(0));
    assertEquals("2024", dataYears(yearRange(map("dataLag", 2))).get(0));

    // dataMonthLag: the axis itself stops at the newest published data year.
    assertEquals("2026", years(yearRange(map("dataMonthLag", 2))).get(0));
    assertEquals("2026", dataYears(yearRange(map("dataMonthLag", 2))).get(0));
  }

  @Test void lagOffsetReportsWhicheverUnitWasDeclared() {
    assertEquals(java.time.Period.ofYears(2), yearRange(map("dataLag", 2)).getLagOffset());
    assertEquals(java.time.Period.ofMonths(2), yearRange(map("dataMonthLag", 2)).getLagOffset());
    assertEquals(java.time.Period.ofYears(0), yearRange(map()).getLagOffset());
  }

  // --- the year boundary ----------------------------------------------------------------------

  /**
   * The case a year-granular lag cannot express, and the one that motivated this: in mid-year a
   * two-month lag stays inside the current year, but in January it must land in the prior one.
   */
  @Test void twoMonthLagCrossesTheYearBoundaryInJanuary() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 6, 15));
    assertEquals("2026", dataYears(yearRange(map("dataMonthLag", 2))).get(0),
        "in June, two months back is still 2026");

    PipelineClock.setOverrideForTest(LocalDate.of(2026, 1, 15));
    assertEquals("2025", dataYears(yearRange(map("dataMonthLag", 2))).get(0),
        "in January, two months back is 2025 — ordinary month arithmetic, not a special case");
  }

  /** A two-month lag must not discard whole years, which is what rounding it up to years did. */
  @Test void monthLagReachesDataYearsAYearLagDiscards() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 8, 31));
    List<String> withMonthLag = dataYears(yearRange(map("dataMonthLag", 2)));
    List<String> withYearLag = dataYears(yearRange(map("dataLag", 2)));

    assertTrue(withMonthLag.contains("2026"), "a 2-month lag reaches 2026 data");
    assertTrue(withMonthLag.contains("2025"), "and 2025");
    assertFalse(withYearLag.contains("2026"), "a 2-year lag reaches neither");
    assertFalse(withYearLag.contains("2025"), "which is the ~2 years of data it discards");
  }

  // --- mutual exclusion -----------------------------------------------------------------------

  @Test void declaringBothLagsFailsAtLoad() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> yearRange(map("dataLag", 1, "dataMonthLag", 6)));
    assertTrue(e.getMessage().contains("mutually exclusive"), e.getMessage());
  }

  /**
   * A stray {@code dataLag: 0} counts as declaring both. It is indistinguishable from absence once
   * the config is built, so accepting it would let a month lag look honoured while a zero-year lag
   * silently governed.
   */
  @Test void aZeroYearLagStillCountsAsDeclaringBoth() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> yearRange(map("dataLag", 0, "dataMonthLag", 2)));
    assertTrue(e.getMessage().contains("rather than setting it to 0"), e.getMessage());
  }

  @Test void negativeMonthLagIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> yearRange(map("dataMonthLag", -1)));
  }

  // --- no regression --------------------------------------------------------------------------

  /** Absent the new key, behavior must be byte-identical to before it existed. */
  @Test void absentMonthLagLeavesYearLagBehaviorUnchanged() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 6, 15));
    // Publish years still run to the current year, and effective_year still trails by the lag.
    assertEquals("2026", years(yearRange(map("dataLag", 2))).get(0));
    assertEquals("2024", dataYears(yearRange(map("dataLag", 2))).get(0));
    assertEquals("2026", dataYears(yearRange(map())).get(0));
  }
}
