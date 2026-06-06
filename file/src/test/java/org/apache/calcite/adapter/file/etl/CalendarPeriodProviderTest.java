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

import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link CalendarPeriodProvider}: calendar-correctness, descending walk,
 * current-period capping, and no future periods. Date is injected for determinism. */
@Tag("unit")
public class CalendarPeriodProviderTest {

  /** Fixed "today": 2026-06-06 (Saturday, Q2, month 6). */
  private static final LocalDate TODAY = LocalDate.of(2026, 6, 6);

  private static Map<String, String> ctx(String... kv) {
    Map<String, String> m = new HashMap<String, String>();
    for (int i = 0; i + 1 < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  private static List<String> values(DimensionType unit, String weekYear,
      Map<String, String> context) {
    return CalendarPeriodProvider.values(unit, weekYear, context, TODAY);
  }

  @Test void monthsFullPriorYearDescending() {
    List<String> m = values(DimensionType.MONTH, null, ctx("year", "2024"));
    assertEquals(12, m.size());
    assertEquals("12", m.get(0));    // descending: newest first
    assertEquals("01", m.get(11));
  }

  @Test void monthsCurrentYearCappedAtCurrentMonth() {
    List<String> m = values(DimensionType.MONTH, null, ctx("year", "2026"));
    assertEquals(Arrays.asList("06", "05", "04", "03", "02", "01"), m);
  }

  @Test void quartersCappedAndDescending() {
    assertEquals(Arrays.asList("2", "1"),
        values(DimensionType.QUARTER, null, ctx("year", "2026")));
    assertEquals(Arrays.asList("4", "3", "2", "1"),
        values(DimensionType.QUARTER, null, ctx("year", "2025")));
  }

  @Test void daysCalendarCorrectLengths() {
    assertEquals(29, values(DimensionType.DAY, null, ctx("year", "2024", "month", "2")).size());
    assertEquals(28, values(DimensionType.DAY, null, ctx("year", "2023", "month", "2")).size());
    assertEquals(30, values(DimensionType.DAY, null, ctx("year", "2024", "month", "4")).size());
    assertEquals(31, values(DimensionType.DAY, null, ctx("year", "2024", "month", "1")).size());
  }

  @Test void daysDescendingZeroPadded() {
    List<String> d = values(DimensionType.DAY, null, ctx("year", "2024", "month", "1"));
    assertEquals("31", d.get(0));
    assertEquals("01", d.get(30));
  }

  @Test void daysCurrentMonthCappedAtToday() {
    assertEquals(Arrays.asList("06", "05", "04", "03", "02", "01"),
        values(DimensionType.DAY, null, ctx("year", "2026", "month", "6")));
  }

  @Test void noFuturePeriods() {
    assertTrue(values(DimensionType.MONTH, null, ctx("year", "2027")).isEmpty());
    assertTrue(values(DimensionType.QUARTER, null, ctx("year", "2027")).isEmpty());
    assertTrue(values(DimensionType.DAY, null, ctx("year", "2026", "month", "7")).isEmpty());
  }

  @Test void weeks52vs53WeekYear() {
    // 2020 is an ISO 53-week year; 2021 has 52.
    assertEquals(53, values(DimensionType.WEEK, "iso", ctx("year", "2020")).size());
    assertEquals(52, values(DimensionType.WEEK, "iso", ctx("year", "2021")).size());
  }

  @Test void weeksCurrentYearCappedAtCurrentIsoWeek() {
    int curWeek = TODAY.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    int curWeekYear = TODAY.get(IsoFields.WEEK_BASED_YEAR);
    List<String> w = values(DimensionType.WEEK, "iso", ctx("year", String.valueOf(curWeekYear)));
    assertEquals(curWeek, w.size());
    String pad = curWeek < 10 ? "0" + curWeek : Integer.toString(curWeek);
    assertEquals(pad, w.get(0));   // descending: current (newest) week first
  }

  @Test void dayOfWeekDescendingOneToSeven() {
    assertEquals(Arrays.asList("7", "6", "5", "4", "3", "2", "1"),
        values(DimensionType.DAY_OF_WEEK, null, Collections.<String, String>emptyMap()));
  }

  @Test void dimensionConfigParsesPeriodKeysFromRawMap() {
    // Mirrors the govdata→file passthrough: a schema-YAML dimension map flows
    // verbatim into DimensionConfig.fromMap (via EtlPipelineConfig/fromDimensionsMap).
    Map<String, Object> raw = new HashMap<String, Object>();
    raw.put("type", "week");
    raw.put("weekYear", "iso");
    raw.put("format", "%02d");
    DimensionConfig c = DimensionConfig.fromMap("week", raw);
    assertEquals(DimensionType.WEEK, c.getType());
    assertEquals("iso", c.getWeekYear());
    assertEquals("%02d", c.getFormat());
  }

  @Test void fromStringParsesPeriodTypes() {
    assertEquals(DimensionType.QUARTER, DimensionType.fromString("quarter"));
    assertEquals(DimensionType.MONTH, DimensionType.fromString("month"));
    assertEquals(DimensionType.WEEK, DimensionType.fromString("week"));
    assertEquals(DimensionType.DAY, DimensionType.fromString("day"));
    assertEquals(DimensionType.DAY_OF_WEEK, DimensionType.fromString("day_of_week"));
    assertTrue(CalendarPeriodProvider.isPeriodUnit(DimensionType.DAY));
    assertTrue(!CalendarPeriodProvider.isPeriodUnit(DimensionType.YEAR_RANGE));
  }
}
