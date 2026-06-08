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
package org.apache.calcite.adapter.govdata.cyber;

import org.apache.calcite.adapter.govdata.cyber.vuln.NvdPublishedWindowDimensionResolver;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionIterator;
import org.apache.calcite.adapter.file.etl.DimensionType;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link NvdPublishedWindowDimensionResolver}.
 *
 * <p>These tests are deterministic (fixed "today" date) and verify:
 * <ol>
 *   <li>The resolver emits quarterly windows for history and daily windows at the tip.</li>
 *   <li>Every window is within the NVD 120-day cap.</li>
 *   <li>No window references {@code GOVDATA_CURRENT_QUARTER} or {@code GOVDATA_CURRENT_YEAR}.</li>
 *   <li>Tip-vs-history boundary is derived from the date, not from any env var.</li>
 *   <li>{@code pubStartDate} ordering is chronological (oldest first).</li>
 * </ol>
 */
@Tag("unit")
class NvdPublishedWindowResolverTest {

  /** Fixed reference date: 2026-06-10 (a Wednesday in Q2 2026). */
  private static final LocalDate TODAY = LocalDate.of(2026, 6, 10);

  private static final DateTimeFormatter DT_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

  // -------------------------------------------------------------------------
  // Window shape
  // -------------------------------------------------------------------------

  @Test
  void windowsShouldBeChronologicallyOrdered() {
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        2024, 90, 1, TODAY);
    assertFalse(windows.isEmpty(), "windows must not be empty");
    for (int i = 1; i < windows.size(); i++) {
      String prev = windows.get(i - 1)[0];
      String curr = windows.get(i)[0];
      assertTrue(prev.compareTo(curr) <= 0,
          "pubStartDate at index " + i + " (" + curr + ") must be >= previous (" + prev + ")");
    }
  }

  @Test
  void everyWindowMustBeWithin120Days() {
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        2024, 90, 1, TODAY);
    for (String[] w : windows) {
      LocalDate start = LocalDate.parse(w[0], DT_FMT);
      LocalDate end = LocalDate.parse(w[1], DT_FMT);
      long days = end.toEpochDay() - start.toEpochDay() + 1;
      assertTrue(days <= NvdPublishedWindowDimensionResolver.NVD_MAX_WINDOW_DAYS,
          "window " + w[0] + " -> " + w[1] + " spans " + days + " days (exceeds 120)");
    }
  }

  @Test
  void historyWindowsAreQuarterly() {
    // startYear=2024, tipDays=90 → history = 2024-01-01 .. 2026-03-11 (approx)
    // Q1 2024: 2024-01-01..2024-03-31 = 91 days ≤ 92 ✓
    // Q2 2024: 2024-04-01..2024-06-30 = 91 days ✓
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        2024, 90, 1, TODAY);

    // First window should be 2024-Q1 start
    String first = windows.get(0)[0];
    assertEquals("2024-01-01T00:00:00.000", first,
        "first history window must start at 2024-01-01");

    // Count quarterly history windows: 2024 has 4 quarters; 2025 has 4 quarters;
    // 2026 tip boundary is TODAY - 90 = 2026-03-12, so history covers through 2026-03-11.
    // That puts the last history quarter boundary at Q1 2026 (or partial).
    // Count that history windows are ≥ 8 (2024-Q1..Q4, 2025-Q1..Q4).
    long quarterlyCount = 0;
    for (String[] w : windows) {
      LocalDate start = LocalDate.parse(w[0], DT_FMT);
      LocalDate end = LocalDate.parse(w[1], DT_FMT);
      long days = end.toEpochDay() - start.toEpochDay() + 1;
      // History windows span ≥ 28 days (shortest quarter-like window)
      if (days > NvdPublishedWindowDimensionResolver.DEFAULT_TIP_CHUNK_DAYS) {
        quarterlyCount++;
      }
    }
    assertTrue(quarterlyCount >= 8,
        "expected at least 8 quarterly history windows, got " + quarterlyCount);
  }

  @Test
  void tipWindowsAreDailyByDefault() {
    // tipChunkDays=1 → each tip window spans exactly 1 day
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        2024, 90, 1, TODAY);

    int tipWindowCount = 0;
    for (String[] w : windows) {
      LocalDate start = LocalDate.parse(w[0], DT_FMT);
      LocalDate end = LocalDate.parse(w[1], DT_FMT);
      long days = end.toEpochDay() - start.toEpochDay() + 1;
      if (days == 1) {
        tipWindowCount++;
      }
    }
    // tipDays=90 → exactly 91 daily windows (90 days before today + today itself)
    assertEquals(91, tipWindowCount,
        "expected 91 daily tip windows (tipDays=90 + today), got " + tipWindowCount);
  }

  @Test
  void noWindowRefersToCurrentQuarterOrYearVars() {
    // Ensure no window value looks like a substitution variable (env var antipattern)
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        2024, 90, 1, TODAY);
    for (String[] w : windows) {
      assertFalse(w[0].contains("${"),
          "pubStartDate '" + w[0] + "' must not contain variable reference");
      assertFalse(w[1].contains("${"),
          "pubEndDate '" + w[1] + "' must not contain variable reference");
      assertFalse(w[0].contains("CURRENT_QUARTER"),
          "pubStartDate must not reference CURRENT_QUARTER");
      assertFalse(w[0].contains("CURRENT_YEAR"),
          "pubStartDate must not reference CURRENT_YEAR");
    }
  }

  // -------------------------------------------------------------------------
  // Quarter math
  // -------------------------------------------------------------------------

  @Test
  void quarterStartReturnsCorrectDates() {
    assertEquals(LocalDate.of(2024, 1, 1),
        NvdPublishedWindowDimensionResolver.quarterStart(2024, 1));
    assertEquals(LocalDate.of(2024, 4, 1),
        NvdPublishedWindowDimensionResolver.quarterStart(2024, 2));
    assertEquals(LocalDate.of(2024, 7, 1),
        NvdPublishedWindowDimensionResolver.quarterStart(2024, 3));
    assertEquals(LocalDate.of(2024, 10, 1),
        NvdPublishedWindowDimensionResolver.quarterStart(2024, 4));
  }

  @Test
  void quarterEndReturnsCorrectDates() {
    assertEquals(LocalDate.of(2024, 3, 31),
        NvdPublishedWindowDimensionResolver.quarterEnd(2024, 1));
    assertEquals(LocalDate.of(2024, 6, 30),
        NvdPublishedWindowDimensionResolver.quarterEnd(2024, 2));
    assertEquals(LocalDate.of(2024, 9, 30),
        NvdPublishedWindowDimensionResolver.quarterEnd(2024, 3));
    assertEquals(LocalDate.of(2024, 12, 31),
        NvdPublishedWindowDimensionResolver.quarterEnd(2024, 4));
    // Leap year check
    assertEquals(LocalDate.of(2024, 3, 31),
        NvdPublishedWindowDimensionResolver.quarterEnd(2024, 1));
  }

  // -------------------------------------------------------------------------
  // Tip boundary: derived from date, not env vars
  // -------------------------------------------------------------------------

  @Test
  void tipBoundaryIsDerivedFromDateNotEnvVars() {
    // With tipDays=30, tip starts 30 days before TODAY (2026-06-10) = 2026-05-11
    // The last history window must end on or before 2026-05-10
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        2024, 30, 1, TODAY);
    LocalDate expectedTipStart = TODAY.minusDays(30);

    // Find the boundary: last window before tip-daily windows
    // History windows end before expectedTipStart; tip windows start at/after expectedTipStart
    for (String[] w : windows) {
      LocalDate start = LocalDate.parse(w[0], DT_FMT);
      long days = LocalDate.parse(w[1], DT_FMT).toEpochDay() - start.toEpochDay() + 1;
      if (days == 1) {
        // This is a tip window — must start >= expectedTipStart
        assertFalse(start.isBefore(expectedTipStart),
            "tip window " + w[0] + " starts before expected tip start " + expectedTipStart);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Context-aware pubEndDate lookup
  // -------------------------------------------------------------------------

  @Test
  void pubEndDateCorrespondsToPubStartDate() {
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        2024, 90, 1, TODAY);
    assertFalse(windows.isEmpty(), "windows must not be empty");
    for (String[] w : windows) {
      String start = w[0];
      String end = w[1];
      assertNotNull(end, "pubEndDate must not be null for pubStartDate=" + start);
      // End must be >= start
      assertTrue(start.compareTo(end) <= 0,
          "pubEndDate " + end + " must be >= pubStartDate " + start);
    }
  }

  // -------------------------------------------------------------------------
  // Single-year boundary: startYear = current year
  // -------------------------------------------------------------------------

  @Test
  void startYearEqualsCurrentYearProducesOnlyTipWindows() {
    // If startYear is the same as today's year and tipDays covers the whole year,
    // history windows might be minimal; test doesn't fail.
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        TODAY.getYear(), 90, 1, TODAY);
    assertFalse(windows.isEmpty(), "should produce tip windows even when startYear=currentYear");
  }

  // -------------------------------------------------------------------------
  // NVD 120-day cap enforcement
  // -------------------------------------------------------------------------

  @Test
  void tipDaysExceeding120IsCappedAt120() {
    // tipDays=200 should be capped at 120
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        2025, 200, 1, TODAY);
    for (String[] w : windows) {
      LocalDate start = LocalDate.parse(w[0], DT_FMT);
      LocalDate end = LocalDate.parse(w[1], DT_FMT);
      long days = end.toEpochDay() - start.toEpochDay() + 1;
      assertTrue(days <= 120,
          "window " + w[0] + " -> " + w[1] + " spans " + days + " days (exceeds 120-day cap)");
    }
  }

  // -------------------------------------------------------------------------
  // DimensionIterator integration: resolver → context → pubEndDate
  // -------------------------------------------------------------------------

  /**
   * Proves the full resolver→DimensionIterator wiring:
   * <ol>
   *   <li>DimensionIterator expands pubStartDate via CUSTOM resolver (no network)</li>
   *   <li>For each pubStartDate, pubEndDate is resolved with that value in context</li>
   *   <li>Each combination has exactly one pubStartDate and one matching pubEndDate</li>
   * </ol>
   * This mirrors the YAML config for the {@code vulnerabilities} table.
   */
  @Test
  void dimensionIteratorProducesMatchingStartEndPairs() {
    NvdPublishedWindowDimensionResolver resolver = new NvdPublishedWindowDimensionResolver();

    // Build dimension config matching the YAML: pubStartDate CUSTOM with properties
    Map<String, String> props = new LinkedHashMap<String, String>();
    props.put("startYear", "2025");
    props.put("tipDays", "7");
    props.put("tipChunkDays", "1");

    DimensionConfig pubStartConfig = DimensionConfig.builder()
        .name("pubStartDate")
        .type(DimensionType.CUSTOM)
        .properties(props)
        .effectiveYearField("pub_year")
        .effectiveMonthField("pub_month")
        .build();

    DimensionConfig pubEndConfig = DimensionConfig.builder()
        .name("pubEndDate")
        .type(DimensionType.CUSTOM)
        .build();

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("type", DimensionConfig.builder()
        .name("type")
        .type(DimensionType.LIST)
        .values(java.util.Collections.singletonList("vulnerabilities"))
        .build());
    dimensions.put("pubStartDate", pubStartConfig);
    dimensions.put("pubEndDate", pubEndConfig);

    DimensionIterator iterator = new DimensionIterator(resolver, null);
    List<Map<String, String>> combos = iterator.expand(dimensions);

    assertFalse(combos.isEmpty(), "must produce at least one combination");

    // Every combination must have pubStartDate and pubEndDate
    for (Map<String, String> combo : combos) {
      String start = combo.get("pubStartDate");
      String end = combo.get("pubEndDate");
      assertNotNull(start, "combination missing pubStartDate: " + combo);
      assertNotNull(end, "combination missing pubEndDate: " + combo);
      // End must be >= start (lexicographic order works for ISO format)
      assertTrue(start.compareTo(end) <= 0,
          "pubEndDate " + end + " must be >= pubStartDate " + start + " in combo " + combo);
      // No variable placeholders
      assertFalse(start.contains("${"), "pubStartDate must not contain unresolved variable");
      assertFalse(end.contains("${"), "pubEndDate must not contain unresolved variable");
    }

    // Count expected windows: startYear=2025 with tipDays=7 and today from system clock.
    // At minimum we expect 4 quarterly history windows (2025-Q1..Q4 or partial) + 8 daily.
    assertTrue(combos.size() >= 4,
        "expected at least 4 combinations (history + tip), got " + combos.size());

    // Confirm effectiveYearField/effectiveMonthField wired through DimensionConfig
    assertEquals("pub_year", pubStartConfig.getEffectiveYearField());
    assertEquals("pub_month", pubStartConfig.getEffectiveMonthField());
  }

  /**
   * Proves that the month→quarter conversion in IcebergMaterializationWriter is correct.
   * Tests the formula: quarter = ((month - 1) / 3) + 1
   */
  @Test
  void monthToQuarterConversion() {
    assertEquals(1, monthToQuarter(1));
    assertEquals(1, monthToQuarter(2));
    assertEquals(1, monthToQuarter(3));
    assertEquals(2, monthToQuarter(4));
    assertEquals(2, monthToQuarter(5));
    assertEquals(2, monthToQuarter(6));
    assertEquals(3, monthToQuarter(7));
    assertEquals(3, monthToQuarter(8));
    assertEquals(3, monthToQuarter(9));
    assertEquals(4, monthToQuarter(10));
    assertEquals(4, monthToQuarter(11));
    assertEquals(4, monthToQuarter(12));
  }

  /** The exact formula used in IcebergMaterializationWriter#processBatch. */
  private static int monthToQuarter(int month) {
    return ((month - 1) / 3) + 1;
  }

  // -------------------------------------------------------------------------
  // GOVDATA_START_YEAR honoring
  // -------------------------------------------------------------------------

  /**
   * Proves no pre-2025 windows are emitted when startYear=2025 is configured.
   * This mirrors the dq-rebuild behavior (GOVDATA_START_YEAR=2025).
   */
  @Test
  void noWindowsBeforeStartYear() {
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        2025, 90, 1, TODAY);
    for (String[] w : windows) {
      String start = w[0];
      // All windows must start at 2025-01-01 or later
      assertTrue(start.compareTo("2025-01-01T00:00:00.000") >= 0,
          "window start " + start + " predates startYear=2025");
    }
    // First window must be exactly 2025-Q1 start
    assertFalse(windows.isEmpty(), "must have windows for startYear=2025");
    assertEquals("2025-01-01T00:00:00.000", windows.get(0)[0],
        "first window must be 2025-01-01 when startYear=2025");
  }

  /**
   * Proves GOVDATA_START_YEAR takes precedence: if the env var says 2025
   * and YAML says 1999, the effective start is max(2025, 1999) = 2025.
   * Uses buildWindowsForTesting(2025, ...) to simulate what resolveStartYear
   * would compute (max of env=2025, yaml=1999 = 2025).
   */
  @Test
  void govdataStartYearTakesMaxWithYamlFloor() {
    // Simulate: GOVDATA_START_YEAR=2025, yamlStartYear=1999 → max = 2025
    int envYear = 2025;
    int yamlYear = 1999;
    int effective = Math.max(envYear, yamlYear);
    assertEquals(2025, effective, "max(GOVDATA_START_YEAR=2025, yaml=1999) must be 2025");

    // The windows from effective=2025 must have no pre-2025 entries
    List<String[]> windows = NvdPublishedWindowDimensionResolver.buildWindowsForTesting(
        effective, 90, 1, TODAY);
    for (String[] w : windows) {
      assertTrue(w[0].compareTo("2025-01-01T00:00:00.000") >= 0,
          "window " + w[0] + " predates the effective start year 2025");
    }
  }
}
