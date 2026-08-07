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

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Table-root {@code releaseWindow:} gate: restricts which calendar months, days of week,
 * and/or year parity a table's ETL pipeline is allowed to run in — for annual/periodic
 * sources whose upstream only publishes new data during a known window (e.g. CMS hospital
 * quality data, published July-October each year).
 *
 * <p>All fields are optional; an absent field places no constraint on that dimension.
 * Faithfully ports the semantics of {@code check-release-window.py}'s {@code check()}
 * function (checked in order: yearParity, then months, then dow — any one failing gates
 * the run).
 *
 * <pre>{@code
 * releaseWindow:
 *   months: [7, 8, 9, 10]   # CMS hospital quality annual release July-October
 *   dow: [1, 3, 5]          # optional: only run Mon/Wed/Fri (0=Sunday .. 6=Saturday)
 *   yearParity: odd         # optional: "odd" or "even" — biennial sources
 * }</pre>
 */
final class ReleaseWindowConfig {

  private final List<Integer> months;
  private final List<Integer> dow;
  private final String yearParity;

  private ReleaseWindowConfig(List<Integer> months, List<Integer> dow, String yearParity) {
    this.months = Collections.unmodifiableList(new ArrayList<Integer>(months));
    this.dow = Collections.unmodifiableList(new ArrayList<Integer>(dow));
    this.yearParity = yearParity;
  }

  /** Calendar months (1=Jan .. 12=Dec) with new data; empty = no constraint. */
  List<Integer> getMonths() {
    return months;
  }

  /** Days of week to run (0=Sunday .. 6=Saturday, matching {@code date +%w}); empty = no constraint. */
  List<Integer> getDow() {
    return dow;
  }

  /** {@code "odd"} or {@code "even"} to further constrain to odd/even calendar years, or null. */
  String getYearParity() {
    return yearParity;
  }

  /**
   * Returns null when {@code today} falls within this release window, or a human-readable
   * reason describing which constraint failed. Checks are independent gates evaluated in the
   * order yearParity, months, dow — mirroring {@code check-release-window.py}'s {@code check()}.
   */
  String checkFailureReason(LocalDate today) {
    if ("odd".equals(yearParity) && today.getYear() % 2 == 0) {
      return "odd-year source, " + today.getYear() + " is even";
    }
    if ("even".equals(yearParity) && today.getYear() % 2 != 0) {
      return "even-year source, " + today.getYear() + " is odd";
    }
    if (!months.isEmpty() && !months.contains(today.getMonthValue())) {
      return "outside release months " + months + " (today: month " + today.getMonthValue() + ")";
    }
    if (!dow.isEmpty()) {
      int todayDow = sundayZeroDow(today);
      if (!dow.contains(todayDow)) {
        return "not a run day (days=" + dow + ", today DOW=" + todayDow + ")";
      }
    }
    return null;
  }

  /** Returns true when {@code today} falls within this release window. */
  boolean isWithinWindow(LocalDate today) {
    return checkFailureReason(today) == null;
  }

  /**
   * Converts {@code today}'s {@link DayOfWeek} (Java: 1=Monday .. 7=Sunday) to the
   * {@code releaseWindow.dow} convention (0=Sunday .. 6=Saturday, matching bash's
   * {@code date +%w} and Python's {@code isoweekday()} conversion). Only Sunday needs
   * remapping — Monday..Saturday already share the same 1..6 values in both conventions.
   */
  private static int sundayZeroDow(LocalDate date) {
    DayOfWeek dow = date.getDayOfWeek();
    return dow == DayOfWeek.SUNDAY ? 0 : dow.getValue();
  }

  /**
   * Parses a {@code releaseWindow:} map, or returns null if absent/empty (no window — the
   * table always runs, which is the default/back-compat behavior for every table that does
   * not declare a {@code releaseWindow:} block).
   */
  static ReleaseWindowConfig fromMap(Map<String, Object> map) {
    if (map == null) {
      return null;
    }
    List<Integer> months = parseIntList(map.get("months"));
    List<Integer> dow = parseIntList(map.get("dow"));
    String yearParity = null;
    Object parityObj = map.get("yearParity");
    if (parityObj instanceof String) {
      yearParity = ((String) parityObj).trim().toLowerCase();
    }
    if (months.isEmpty() && dow.isEmpty() && yearParity == null) {
      return null;
    }
    return new ReleaseWindowConfig(months, dow, yearParity);
  }

  private static List<Integer> parseIntList(Object obj) {
    List<Integer> result = new ArrayList<Integer>();
    if (obj instanceof List) {
      for (Object item : (List<?>) obj) {
        if (item instanceof Number) {
          result.add(((Number) item).intValue());
        }
      }
    }
    return result;
  }
}
