/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.etl;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Built-in calendar-aware providers for the canonical period dimensions
 * ({@code quarter}, {@code month}, {@code week}, {@code day}, {@code day_of_week}).
 *
 * <p>Values are calendar-correct — leap days (Feb 28/29), 30/31-day months,
 * 52/53 ISO weeks — and <b>capped at the current (open) period</b>: no future
 * period is ever emitted, and the still-filling current period is included.
 * Coarser dimensions are read from the resolution context: a {@code day}
 * provider needs {@code year} and {@code month}; {@code quarter}/{@code month}/
 * {@code week} need {@code year}.
 *
 * <p>Values are emitted in <b>descending</b> order (newest period first),
 * matching {@code YEAR_RANGE}, so the dimension cross-product walks the most
 * recent period first.
 *
 * <p>Emitted values are the <b>canonical, sortable</b> form (zero-padded where
 * needed) used as completion-marker and partition keys. Rendering for API
 * substitution is a separate concern (the dimension {@code format}); this
 * provider never applies it.
 *
 * <p>All calendar math is UTC-based; the "current date" is injected so callers
 * (and tests) control it rather than reading a wall clock here.
 */
final class CalendarPeriodProvider {

  /** Week numbering: {@code year} is the ISO-8601 week-based-year. */
  static final String WEEK_YEAR_ISO = "iso";
  /** Week numbering: {@code year} is the calendar year (week-of-year). */
  static final String WEEK_YEAR_CALENDAR = "calendar";

  private CalendarPeriodProvider() {
  }

  /**
   * Resolves the canonical values for a period dimension given context.
   *
   * @param unit     one of {@code QUARTER, MONTH, WEEK, DAY, DAY_OF_WEEK}
   * @param weekYear week-year mode for {@code WEEK} ({@link #WEEK_YEAR_ISO} or
   *                 {@link #WEEK_YEAR_CALENDAR}); ignored for other units
   * @param context  already-resolved coarser dimensions (e.g. {@code year}, {@code month})
   * @param today    current date (UTC in production; injected for tests)
   * @return canonical zero-padded values, calendar-correct, capped at {@code today}
   */
  static List<String> values(DimensionType unit, String weekYear,
      Map<String, String> context, LocalDate today) {
    switch (unit) {
    case QUARTER:
      return quarters(context, today);
    case MONTH:
      return months(context, today);
    case WEEK:
      return weeks(context, weekYear, today);
    case DAY:
      return days(context, today);
    case DAY_OF_WEEK:
      return daysOfWeek();
    default:
      return new ArrayList<String>();
    }
  }

  /** True if {@code unit} is one of the canonical calendar period units. */
  static boolean isPeriodUnit(DimensionType unit) {
    return unit == DimensionType.QUARTER || unit == DimensionType.MONTH
        || unit == DimensionType.WEEK || unit == DimensionType.DAY
        || unit == DimensionType.DAY_OF_WEEK;
  }

  private static List<String> quarters(Map<String, String> context, LocalDate today) {
    Integer year = intOrNull(context.get("year"));
    int max = 4;
    if (year != null) {
      if (year > today.getYear()) {
        return new ArrayList<String>();
      }
      if (year == today.getYear()) {
        max = ((today.getMonthValue() - 1) / 3) + 1;
      }
    }
    return seq(1, max, 1);
  }

  private static List<String> months(Map<String, String> context, LocalDate today) {
    Integer year = intOrNull(context.get("year"));
    int max = 12;
    if (year != null) {
      if (year > today.getYear()) {
        return new ArrayList<String>();
      }
      if (year == today.getYear()) {
        max = today.getMonthValue();
      }
    }
    return seq(1, max, 2);
  }

  private static List<String> days(Map<String, String> context, LocalDate today) {
    Integer year = intOrNull(context.get("year"));
    Integer month = intOrNull(context.get("month"));
    if (month == null) {
      // day without a month dimension is a schema misconfiguration; fall back to
      // the longest possible month rather than emitting nothing.
      return seq(1, 31, 2);
    }
    int effYear = (year != null) ? year : today.getYear();
    int len = YearMonth.of(effYear, month).lengthOfMonth();
    if (year != null) {
      if (year > today.getYear()
          || (year == today.getYear() && month > today.getMonthValue())) {
        return new ArrayList<String>();
      }
      if (year == today.getYear() && month == today.getMonthValue()) {
        len = today.getDayOfMonth();
      }
    }
    return seq(1, len, 2);
  }

  private static List<String> weeks(Map<String, String> context, String weekYear,
      LocalDate today) {
    Integer year = intOrNull(context.get("year"));
    boolean calendar = WEEK_YEAR_CALENDAR.equalsIgnoreCase(weekYear);
    if (year == null) {
      return seq(1, 53, 2);
    }
    // Dec 28 always falls in the final ISO week of its week-based-year, which
    // equals the calendar year — so this yields 52 or 53 for either mode.
    int weeksInYear = LocalDate.of(year, 12, 28).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    int curWeekYear = calendar ? today.getYear() : today.get(IsoFields.WEEK_BASED_YEAR);
    int curWeek = calendar
        ? clampToYear(today)
        : today.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    if (year > curWeekYear) {
      return new ArrayList<String>();
    }
    int max = (year == curWeekYear) ? Math.min(curWeek, weeksInYear) : weeksInYear;
    return seq(1, max, 2);
  }

  /** Week-of-year for {@code today} bounded to its own calendar year (calendar mode). */
  private static int clampToYear(LocalDate today) {
    int isoWeek = today.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    int isoWeekYear = today.get(IsoFields.WEEK_BASED_YEAR);
    if (isoWeekYear < today.getYear()) {
      // early-January date whose ISO week belongs to the prior week-year → week 1
      return 1;
    }
    if (isoWeekYear > today.getYear()) {
      // late-December date whose ISO week rolled into next week-year → last week
      return LocalDate.of(today.getYear(), 12, 28).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    }
    return isoWeek;
  }

  private static List<String> daysOfWeek() {
    return seq(1, 7, 1);
  }

  /**
   * Inclusive integer sequence rendered with {@code width} zero-padding, emitted
   * in <b>descending</b> order. Periods always walk newest-first (matching
   * {@code YEAR_RANGE}), so the cross-product processes the most-recent period
   * first regardless of the per-dimension {@code descending} flag.
   */
  private static List<String> seq(int from, int to, int width) {
    List<String> out = new ArrayList<String>();
    for (int i = to; i >= from; i--) {
      out.add(pad(i, width));
    }
    return out;
  }

  private static String pad(int value, int width) {
    if (width <= 1) {
      return Integer.toString(value);
    }
    String s = Integer.toString(value);
    StringBuilder sb = new StringBuilder();
    for (int i = s.length(); i < width; i++) {
      sb.append('0');
    }
    return sb.append(s).toString();
  }

  private static Integer intOrNull(String value) {
    if (value == null) {
      return null;
    }
    try {
      return Integer.valueOf(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
