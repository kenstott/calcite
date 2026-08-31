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

import java.time.LocalDate;

/**
 * The single resolution point for "now" across dimension resolution and freshness evaluation.
 *
 * <p>Which periods an ETL run requests is pure arithmetic over the current date, {@code dataLag},
 * {@code dataMonthLag} and {@code releaseMonth}. Reading the clock directly at each site makes that
 * arithmetic untestable except on the day the behavior happens to occur — the year-boundary cases
 * only arise in January, and any test written against the real clock silently changes meaning as
 * the calendar advances. Routing every read through here lets a test pin an arbitrary date and
 * assert the generated combinations offline, with no network and no dependency on what a source
 * has published.
 *
 * <p>The override is process-wide and defaults to absent, in which case every accessor is exactly
 * {@link LocalDate#now()}. It is set from the {@code pipelineDate} model operand (never from the
 * environment directly) so a simulated date is declared in the model like any other input, and
 * from {@link #setOverrideForTest} in tests.
 *
 * <p><b>Month values are 1-based</b> ({@code January == 1}), matching {@link LocalDate}. Callers
 * that need the last completed month must say so explicitly via {@link #lastCompletedMonth()}
 * rather than relying on a zero-based calendar field to mean it implicitly.
 */
public final class PipelineClock {

  /** Process-wide simulated date; null means use the real clock. */
  private static volatile LocalDate override;

  private PipelineClock() {
  }

  /** The current date — the simulated one when set, otherwise the real clock. */
  public static LocalDate today() {
    LocalDate pinned = override;
    return pinned != null ? pinned : LocalDate.now();
  }

  /** Current calendar year. */
  public static int currentYear() {
    return today().getYear();
  }

  /** Current calendar month, <b>1-based</b> (January == 1). */
  public static int currentMonth() {
    return today().getMonthValue();
  }

  /**
   * The number of the most recently completed month, 1-based; {@code 0} in January, when no month
   * of the current year has completed yet.
   *
   * <p>Stated as its own accessor because the distinction between "current month" and "last
   * completed month" is a correctness boundary that an off-by-one at the call site hides.
   */
  public static int lastCompletedMonth() {
    return currentMonth() - 1;
  }

  /**
   * Pins the date for the remainder of the process, or clears it when passed null.
   *
   * @param date the simulated date, or null to resume using the real clock
   */
  public static void setOverride(LocalDate date) {
    override = date;
  }

  /**
   * Pins the date for a test. Tests must clear this in teardown — the override is process-wide, so
   * a leaked value silently changes every subsequent test in the same JVM.
   *
   * @param date the simulated date
   */
  public static void setOverrideForTest(LocalDate date) {
    override = date;
  }

  /** Restores the real clock. */
  public static void clearOverride() {
    override = null;
  }

  /** True when a simulated date is in effect. */
  public static boolean isOverridden() {
    return override != null;
  }
}
