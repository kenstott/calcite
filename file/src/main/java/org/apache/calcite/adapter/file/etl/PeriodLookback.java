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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves which previously-published periods a run should reopen.
 *
 * <p>The lookback walks backwards from the current period over periods the pipeline has
 * <em>actually published</em>, collecting the {@code lookbackPeriods} most recent ones. It performs
 * no lag or calendar-window arithmetic: {@code dataLag} and {@code dataMonthLag} govern what is
 * available to fetch, this governs what is worth re-checking, and the two are independent. Anchoring
 * on published data rather than on the clock means every period in the returned set is one that
 * demonstrably exists, so the set can never contain an empty future period.
 *
 * <p>Only completed periods are considered. A period that was never ingested is already unprocessed
 * and is reached by the ordinary path, so it is stepped over here without consuming budget — which
 * also means a table with gaps still re-checks {@code N} real periods rather than spending the
 * budget on holes.
 *
 * <p>The walk steps in the finest canonical period unit the table declares, so a month-grain table
 * steps by month and a year-grain table by year. Roll-under across a year boundary is ordinary date
 * arithmetic rather than a special case.
 */
final class PeriodLookback {

  private static final Logger LOGGER = LoggerFactory.getLogger(PeriodLookback.class);

  /**
   * Hard cap on how far the walk probes before giving up, so a table with little or nothing
   * published cannot walk to the beginning of time. Generous relative to any real lookback.
   */
  private static final int MAX_STEPS = 500;

  /** The step unit, finest-first; mirrors {@link IncrementalTracker#PERIOD_SLOTS}. */
  private enum Grain { DAY, MONTH, QUARTER, YEAR }

  private PeriodLookback() {
  }

  /**
   * Returns the period-value maps for the {@code lookbackPeriods} most recently published periods,
   * newest first. Empty when the table has published nothing, when no canonical period slot is
   * declared, or when the lookback is not configured.
   *
   * @param pipelineName schema-qualified pipeline name, as used for period-completion keys
   * @param dimensions the table's dimension configs, read for which period slots are in play
   * @param lookbackPeriods how many published periods to collect; null or &lt; 1 returns empty
   * @param floorYear lowest year the walk may reach, from the dimension's start / minYear
   * @param tracker source of truth for whether a period was published
   * @return period-value maps to reopen, newest first
   */
  static List<Map<String, String>> resolve(String pipelineName,
      Map<String, DimensionConfig> dimensions, Integer lookbackPeriods, int floorYear,
      IncrementalTracker tracker) {
    if (lookbackPeriods == null || lookbackPeriods < 1 || tracker == null) {
      return new ArrayList<Map<String, String>>();
    }
    Set<String> slots = declaredSlots(dimensions);
    if (slots.isEmpty()) {
      // Not period-tracked: periodCompletionKey would be all-NA and every period would collide.
      // Rejected at config load; defensive here.
      return new ArrayList<Map<String, String>>();
    }
    Grain grain = finestGrain(slots);

    LocalDate cursorDate = PipelineClock.today();
    int year = cursorDate.getYear();
    int sub = subUnit(cursorDate, grain);

    List<Map<String, String>> found = new ArrayList<Map<String, String>>();
    int steps = 0;
    while (found.size() < lookbackPeriods && steps < MAX_STEPS && year >= floorYear) {
      Map<String, String> period = periodValues(slots, year, sub, grain);
      if (tracker.isPeriodComplete(pipelineName, period)) {
        found.add(period);
      }
      // Step back one unit of the finest declared grain; roll under into the prior year.
      switch (grain) {
      case YEAR:
        year--;
        break;
      case QUARTER:
        if (--sub < 1) {
          sub = 4;
          year--;
        }
        break;
      case MONTH:
      case DAY:
      default:
        if (--sub < 1) {
          sub = 12;
          year--;
        }
        break;
      }
      steps++;
    }

    if (found.isEmpty()) {
      LOGGER.debug("Lookback for '{}': no published periods found within {} steps (floor {})",
          pipelineName, steps, floorYear);
    } else {
      LOGGER.info("Lookback for '{}': reopening {} published period(s) of {} requested "
          + "(grain={}, {} probes)", pipelineName, found.size(), lookbackPeriods, grain, steps);
    }
    return found;
  }

  /**
   * The lowest year in a resolved lookback set, or {@link Integer#MAX_VALUE} when empty — the value
   * a caller extends its year range down to so the set is actually generated.
   */
  static int oldestYear(List<Map<String, String>> periods) {
    int oldest = Integer.MAX_VALUE;
    for (Map<String, String> p : periods) {
      String y = p.get("year");
      if (y == null) {
        continue;
      }
      try {
        oldest = Math.min(oldest, Integer.parseInt(y));
      } catch (NumberFormatException e) {
        continue;
      }
    }
    return oldest;
  }

  /** Canonical period slots this table declares as dimensions. */
  private static Set<String> declaredSlots(Map<String, DimensionConfig> dimensions) {
    Set<String> slots = new LinkedHashSet<String>();
    if (dimensions == null) {
      return slots;
    }
    for (String slot : IncrementalTracker.PERIOD_SLOTS) {
      if (dimensions.containsKey(slot)) {
        slots.add(slot);
      }
    }
    return slots;
  }

  /** The finest declared grain — one step of the walk. */
  private static Grain finestGrain(Set<String> slots) {
    if (slots.contains("day")) {
      return Grain.DAY;
    }
    if (slots.contains("month")) {
      return Grain.MONTH;
    }
    if (slots.contains("quarter")) {
      return Grain.QUARTER;
    }
    return Grain.YEAR;
  }

  /** The sub-year component of {@code date} at the given grain. */
  private static int subUnit(LocalDate date, Grain grain) {
    switch (grain) {
    case QUARTER:
      return ((date.getMonthValue() - 1) / 3) + 1;
    case MONTH:
    case DAY:
      return date.getMonthValue();
    case YEAR:
    default:
      return 0;
    }
  }

  /**
   * Builds the period-value map for one candidate, populating only the slots the table declares so
   * the key matches what {@code markPeriodComplete} wrote.
   */
  private static Map<String, String> periodValues(Set<String> slots, int year, int sub,
      Grain grain) {
    Map<String, String> values = new LinkedHashMap<String, String>();
    if (slots.contains("year")) {
      values.put("year", String.valueOf(year));
    }
    if (slots.contains("quarter") && grain == Grain.QUARTER) {
      values.put("quarter", String.valueOf(sub));
    }
    if (slots.contains("month") && (grain == Grain.MONTH || grain == Grain.DAY)) {
      values.put("month", sub < 10 ? "0" + sub : String.valueOf(sub));
    }
    return values;
  }
}
