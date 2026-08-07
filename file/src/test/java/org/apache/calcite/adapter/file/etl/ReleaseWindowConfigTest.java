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
import java.time.Month;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ReleaseWindowConfig} parsing and window-check logic.
 *
 * <p>Uses fixed, hand-picked {@link LocalDate} fixtures rather than {@code LocalDate.now()} so
 * assertions never depend on the date the test happens to run.
 */
@Tag("unit")
class ReleaseWindowConfigTest {

  // 2024-08-15 is a Thursday (config dow convention: 0=Sun..6=Sat, so Thursday=4), even year.
  private static final LocalDate THURSDAY_AUG_2024 = LocalDate.of(2024, Month.AUGUST, 15);
  // 2024-08-11 is a Sunday (config dow=0), even year.
  private static final LocalDate SUNDAY_AUG_2024 = LocalDate.of(2024, Month.AUGUST, 11);
  // 2025-08-14 is a Thursday, odd year.
  private static final LocalDate THURSDAY_AUG_2025 = LocalDate.of(2025, Month.AUGUST, 14);
  // 2024-03-10 is a Sunday, even year, month=3 (outside a July-October window).
  private static final LocalDate SUNDAY_MARCH_2024 = LocalDate.of(2024, Month.MARCH, 10);

  // ===== fromMap parsing =====

  @Test void testFromMapNullReturnsNull() {
    assertNull(ReleaseWindowConfig.fromMap(null));
  }

  @Test void testFromMapEmptyMapReturnsNull() {
    assertNull(ReleaseWindowConfig.fromMap(Collections.<String, Object>emptyMap()),
        "An empty releaseWindow: map has no constraints — treat as absent (always run)");
  }

  @Test void testFromMapParsesMonths() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("months", Arrays.asList(7, 8, 9, 10));
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(map);
    assertEquals(Arrays.asList(7, 8, 9, 10), config.getMonths());
    assertTrue(config.getDow().isEmpty());
    assertNull(config.getYearParity());
  }

  @Test void testFromMapParsesDow() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("dow", Arrays.asList(1, 3, 5));
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(map);
    assertEquals(Arrays.asList(1, 3, 5), config.getDow());
    assertTrue(config.getMonths().isEmpty());
  }

  @Test void testFromMapParsesYearParity() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("yearParity", "odd");
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(map);
    assertEquals("odd", config.getYearParity());
  }

  @Test void testFromMapNormalizesYearParityCase() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("yearParity", "ODD");
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(map);
    assertEquals("odd", config.getYearParity());
  }

  // ===== months constraint =====

  @Test void testMonthsInWindowProceeds() {
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("months", Arrays.asList(7, 8, 9, 10)));
    assertTrue(config.isWithinWindow(THURSDAY_AUG_2024), "August is within [7,8,9,10]");
    assertNull(config.checkFailureReason(THURSDAY_AUG_2024));
  }

  @Test void testMonthsOutOfWindowSkips() {
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("months", Arrays.asList(7, 8, 9, 10)));
    assertFalse(config.isWithinWindow(SUNDAY_MARCH_2024), "March is outside [7,8,9,10]");
    String reason = config.checkFailureReason(SUNDAY_MARCH_2024);
    assertTrue(reason.contains("release months"), "Reason should name the months constraint: " + reason);
    assertTrue(reason.contains("month 3"), "Reason should report today's actual month: " + reason);
  }

  // ===== dow constraint =====

  @Test void testDowInWindowProceeds() {
    // Thursday = 4 in the 0=Sun..6=Sat convention.
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("dow", Arrays.asList(2, 4)));
    assertTrue(config.isWithinWindow(THURSDAY_AUG_2024));
  }

  @Test void testDowOutOfWindowSkips() {
    // Only Mon(1)/Wed(3)/Fri(5) allowed; Thursday(4) is not.
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("dow", Arrays.asList(1, 3, 5)));
    assertFalse(config.isWithinWindow(THURSDAY_AUG_2024));
    String reason = config.checkFailureReason(THURSDAY_AUG_2024);
    assertTrue(reason.contains("not a run day"), reason);
    assertTrue(reason.contains("DOW=4"), "Thursday must map to DOW=4: " + reason);
  }

  @Test void testSundayMapsToZero() {
    // Sunday must map to DOW=0 (not 7) — the off-by-one this port must get right.
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("dow", Collections.singletonList(0)));
    assertTrue(config.isWithinWindow(SUNDAY_AUG_2024), "Sunday must satisfy dow=[0]");

    ReleaseWindowConfig excludesSunday = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("dow", Arrays.asList(1, 2, 3, 4, 5, 6)));
    assertFalse(excludesSunday.isWithinWindow(SUNDAY_AUG_2024));
    assertTrue(excludesSunday.checkFailureReason(SUNDAY_AUG_2024).contains("DOW=0"));
  }

  // ===== yearParity constraint =====

  @Test void testYearParityOddProceedsOnOddYear() {
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("yearParity", "odd"));
    assertTrue(config.isWithinWindow(THURSDAY_AUG_2025));
  }

  @Test void testYearParityOddSkipsOnEvenYear() {
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("yearParity", "odd"));
    assertFalse(config.isWithinWindow(THURSDAY_AUG_2024));
    assertTrue(config.checkFailureReason(THURSDAY_AUG_2024).contains("odd-year"));
  }

  @Test void testYearParityEvenProceedsOnEvenYear() {
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("yearParity", "even"));
    assertTrue(config.isWithinWindow(THURSDAY_AUG_2024));
  }

  @Test void testYearParityEvenSkipsOnOddYear() {
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("yearParity", "even"));
    assertFalse(config.isWithinWindow(THURSDAY_AUG_2025));
    assertTrue(config.checkFailureReason(THURSDAY_AUG_2025).contains("even-year"));
  }

  // ===== combined constraints (each independently gates) =====

  @Test void testCombinedMonthsAndDowBothSatisfiedProceeds() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("months", Arrays.asList(7, 8, 9, 10));
    map.put("dow", Collections.singletonList(4)); // Thursday
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(map);
    assertTrue(config.isWithinWindow(THURSDAY_AUG_2024), "August + Thursday satisfies both");
  }

  @Test void testCombinedMonthsFailsIndependentlyOfDow() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("months", Arrays.asList(7, 8, 9, 10));
    map.put("dow", Collections.singletonList(0)); // Sunday — matches SUNDAY_MARCH_2024's DOW
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(map);
    // dow matches (Sunday) but month (March) does not — months constraint alone must gate.
    assertFalse(config.isWithinWindow(SUNDAY_MARCH_2024));
    assertTrue(config.checkFailureReason(SUNDAY_MARCH_2024).contains("release months"));
  }

  @Test void testCombinedDowFailsIndependentlyOfMonths() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("months", Arrays.asList(7, 8, 9, 10)); // August satisfies this
    map.put("dow", Collections.singletonList(0)); // Sunday only; Thursday does not satisfy
    ReleaseWindowConfig config = ReleaseWindowConfig.fromMap(map);
    assertFalse(config.isWithinWindow(THURSDAY_AUG_2024));
    assertTrue(config.checkFailureReason(THURSDAY_AUG_2024).contains("not a run day"));
  }

  // ===== no releaseWindow configured =====

  @Test void testAbsentReleaseWindowConfigMeansNoConstraint() {
    // fromMap already returns null for empty/absent — EtlPipelineConfig.getReleaseWindow()
    // being null is the "always run" signal EtlPipeline checks.
    assertNull(ReleaseWindowConfig.fromMap(Collections.<String, Object>emptyMap()));
  }
}
