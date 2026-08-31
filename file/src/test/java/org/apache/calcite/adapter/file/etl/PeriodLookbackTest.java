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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the lookback selects previously-published periods, counted in periods rather than
 * calendar intervals.
 *
 * <p>The properties worth pinning are the ones that were wrong in earlier designs: that the walk is
 * anchored on published data rather than the clock, that a sparse or cadenced axis needs no
 * inflated value, that gaps do not consume the budget, and that lag configuration cannot influence
 * the result.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(PipelineClockTest.CLOCK_LOCK)
public class PeriodLookbackTest {

  private static final String PIPE = "econ.demo";

  @AfterEach void unpin() {
    PipelineClock.clearOverride();
  }

  /**
   * Tracker whose completed-period set is supplied by the test. Only period completion is
   * meaningful here; every other operation delegates to the no-op tracker.
   */
  private static final class StubTracker implements IncrementalTracker {
    private final Set<String> complete = new HashSet<String>();

    StubTracker(String... keys) {
      complete.addAll(Arrays.asList(keys));
    }

    @Override public boolean isPeriodComplete(String pipelineName,
        Map<String, String> periodValues) {
      return complete.contains(IncrementalTracker.periodCompletionKey(pipelineName, periodValues));
    }

    @Override public boolean isProcessed(String a, String s, Map<String, String> k) {
      return IncrementalTracker.NOOP.isProcessed(a, s, k);
    }

    @Override public boolean isProcessedWithTtl(String a, String s, Map<String, String> k,
        long ttl) {
      return IncrementalTracker.NOOP.isProcessedWithTtl(a, s, k, ttl);
    }

    @Override public void markProcessed(String a, String s, Map<String, String> k, String p) {
    }

    @Override public Set<Map<String, String>> getProcessedKeyValues(String a) {
      return IncrementalTracker.NOOP.getProcessedKeyValues(a);
    }

    @Override public void invalidate(String a, Map<String, String> k) {
    }

    @Override public void invalidateAll(String a) {
    }

    @Override public Set<Integer> filterUnprocessed(String a, String s,
        List<Map<String, String>> all) {
      return IncrementalTracker.NOOP.filterUnprocessed(a, s, all);
    }

    @Override public boolean isTableComplete(String p, String d) {
      return false;
    }

    @Override public void markTableComplete(String p, String d) {
    }

    @Override public void invalidateTableCompletion(String p) {
    }

    @Override public void clearAllCompletions() {
    }
  }

  private static Map<String, DimensionConfig> yearOnly() {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder().name("year").type(DimensionType.YEAR_RANGE).build());
    return dims;
  }

  private static Map<String, DimensionConfig> yearMonth() {
    Map<String, DimensionConfig> dims = yearOnly();
    dims.put("month", DimensionConfig.builder().name("month").type(DimensionType.LIST).build());
    return dims;
  }

  private static String yearKey(int year) {
    Map<String, String> p = new LinkedHashMap<String, String>();
    p.put("year", String.valueOf(year));
    return IncrementalTracker.periodCompletionKey(PIPE, p);
  }

  private static String yearMonthKey(int year, int month) {
    Map<String, String> p = new LinkedHashMap<String, String>();
    p.put("year", String.valueOf(year));
    p.put("month", month < 10 ? "0" + month : String.valueOf(month));
    return IncrementalTracker.periodCompletionKey(PIPE, p);
  }

  @Test void collectsTheMostRecentPublishedYearsNewestFirst() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 6, 15));
    StubTracker t = new StubTracker(yearKey(2025), yearKey(2024), yearKey(2023), yearKey(2022));

    List<Map<String, String>> got =
        PeriodLookback.resolve(PIPE, yearOnly(), 3, 2010, t);

    assertEquals(3, got.size());
    assertEquals("2025", got.get(0).get("year"));
    assertEquals("2024", got.get(1).get("year"));
    assertEquals("2023", got.get(2).get("year"));
  }

  /** A never-ingested table has nothing to reopen; the lookback acts only on completed periods. */
  @Test void coldTableYieldsAnEmptySet() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 6, 15));
    assertTrue(PeriodLookback.resolve(PIPE, yearOnly(), 6, 2010, new StubTracker()).isEmpty());
  }

  /**
   * Gaps are stepped over rather than counted, so a table with holes still re-checks N real
   * periods instead of spending the budget on periods that were never ingested.
   */
  @Test void gapsDoNotConsumeTheBudget() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 6, 15));
    // 2025 and 2024 were never ingested.
    StubTracker t = new StubTracker(yearKey(2026), yearKey(2023), yearKey(2022));

    List<Map<String, String>> got = PeriodLookback.resolve(PIPE, yearOnly(), 3, 2010, t);

    assertEquals(3, got.size());
    assertEquals(Arrays.asList("2026", "2023", "2022"),
        Arrays.asList(got.get(0).get("year"), got.get(1).get("year"), got.get(2).get("year")));
  }

  /**
   * A five-year stepped axis needs no inflated value: counting published periods finds both real
   * editions with N=2, where a calendar-year window would have needed 8.
   */
  @Test void steppedAxisNeedsNoInflatedValue() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 6, 15));
    StubTracker t = new StubTracker(yearKey(2022), yearKey(2017), yearKey(2012));

    List<Map<String, String>> got = PeriodLookback.resolve(PIPE, yearOnly(), 2, 2010, t);

    assertEquals(2, got.size(), "must find the two most recent published editions");
    assertEquals("2022", got.get(0).get("year"));
    assertEquals("2017", got.get(1).get("year"));
  }

  /** An even-year cadence is invisible to a period walk, including from an odd calendar year. */
  @Test void evenYearCadenceResolvesFromAnOddYear() {
    PipelineClock.setOverrideForTest(LocalDate.of(2027, 3, 1));
    StubTracker t = new StubTracker(yearKey(2026), yearKey(2024), yearKey(2022));

    List<Map<String, String>> got = PeriodLookback.resolve(PIPE, yearOnly(), 2, 2010, t);

    assertEquals(Arrays.asList("2026", "2024"),
        Arrays.asList(got.get(0).get("year"), got.get(1).get("year")));
  }

  /** A month-grain table steps by month, rolling under the year boundary as ordinary arithmetic. */
  @Test void monthGrainWalksByMonthAcrossTheYearBoundary() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 2, 10));
    StubTracker t = new StubTracker(
        yearMonthKey(2026, 1), yearMonthKey(2025, 12), yearMonthKey(2025, 11));

    List<Map<String, String>> got = PeriodLookback.resolve(PIPE, yearMonth(), 3, 2010, t);

    assertEquals(3, got.size());
    assertEquals("2026", got.get(0).get("year"));
    assertEquals("01", got.get(0).get("month"));
    assertEquals("2025", got.get(1).get("year"));
    assertEquals("12", got.get(1).get("month"));
    assertEquals("2025", got.get(2).get("year"));
    assertEquals("11", got.get(2).get("month"));
  }

  /** The floor bounds the walk so a sparse table cannot probe indefinitely. */
  @Test void walkStopsAtTheFloor() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 6, 15));
    StubTracker t = new StubTracker(yearKey(2011), yearKey(2005));

    List<Map<String, String>> got = PeriodLookback.resolve(PIPE, yearOnly(), 5, 2010, t);

    assertEquals(1, got.size(), "2005 is below the floor and must not be reached");
    assertEquals("2011", got.get(0).get("year"));
  }

  @Test void absentLookbackYieldsAnEmptySet() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 6, 15));
    StubTracker t = new StubTracker(yearKey(2025));
    assertTrue(PeriodLookback.resolve(PIPE, yearOnly(), null, 2010, t).isEmpty());
  }

  @Test void oldestYearReportsTheRangeFloorNeededToGenerateTheSet() {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 6, 15));
    StubTracker t = new StubTracker(yearKey(2025), yearKey(2021));
    assertEquals(2021, PeriodLookback.oldestYear(
        PeriodLookback.resolve(PIPE, yearOnly(), 2, 2010, t)));
  }
}
