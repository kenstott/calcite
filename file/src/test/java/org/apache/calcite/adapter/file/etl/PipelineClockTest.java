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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the injectable pipeline date.
 *
 * <p>The value of this seam is that period-generation arithmetic becomes assertable at an arbitrary
 * date, offline. The two properties that matter are that pinning a date works, and that <em>not</em>
 * pinning one leaves behavior exactly as it was — the second is what makes the change safe to apply
 * to every dimension in the system at once.
 */
@Tag("unit")
// The override is process-wide state. Test classes run concurrently by default in this module
// (junit.jupiter.execution.parallel.mode.classes.default=concurrent), so @AfterEach alone does not
// prevent one class from observing another's pin. The lock serialises every test that touches it.
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(PipelineClockTest.CLOCK_LOCK)
public class PipelineClockTest {

  /** Shared lock name — any test that pins the pipeline date must declare it. */
  public static final String CLOCK_LOCK = "pipeline-clock";

  @AfterEach void clearPin() {
    // The override is process-wide; a leaked value silently changes every later test in this JVM.
    PipelineClock.clearOverride();
  }

  @Test void withNoOverrideTracksTheRealClock() {
    assertFalse(PipelineClock.isOverridden(), "no override should be set by default");
    LocalDate now = LocalDate.now();
    assertEquals(now.getYear(), PipelineClock.currentYear());
    assertEquals(now.getMonthValue(), PipelineClock.currentMonth());
  }

  @Test void pinnedDateIsReportedInsteadOfTheRealClock() {
    PipelineClock.setOverrideForTest(LocalDate.of(2021, 3, 17));
    assertTrue(PipelineClock.isOverridden());
    assertEquals(LocalDate.of(2021, 3, 17), PipelineClock.today());
    assertEquals(2021, PipelineClock.currentYear());
    assertEquals(3, PipelineClock.currentMonth());
  }

  @Test void monthIsOneBased() {
    PipelineClock.setOverrideForTest(LocalDate.of(2024, 1, 9));
    assertEquals(1, PipelineClock.currentMonth(), "January must be 1, not 0");
    PipelineClock.setOverrideForTest(LocalDate.of(2024, 12, 9));
    assertEquals(12, PipelineClock.currentMonth(), "December must be 12, not 11");
  }

  /**
   * The monthlyYTD ceiling previously depended on {@code Calendar.MONTH} being zero-based to mean
   * "last completed month". Normalising to 1-based months only stays correct if that meaning is
   * carried explicitly, so it is asserted here rather than left implicit at the call site.
   */
  @Test void lastCompletedMonthIsZeroInJanuaryAndOneBehindOtherwise() {
    PipelineClock.setOverrideForTest(LocalDate.of(2024, 1, 31));
    assertEquals(0, PipelineClock.lastCompletedMonth(),
        "in January no month of the current year has completed");

    PipelineClock.setOverrideForTest(LocalDate.of(2024, 6, 1));
    assertEquals(5, PipelineClock.lastCompletedMonth(), "in June, May is the last closed month");

    PipelineClock.setOverrideForTest(LocalDate.of(2024, 12, 31));
    assertEquals(11, PipelineClock.lastCompletedMonth());
  }

  @Test void clearingRestoresTheRealClock() {
    PipelineClock.setOverrideForTest(LocalDate.of(1999, 7, 4));
    assertEquals(1999, PipelineClock.currentYear());
    PipelineClock.clearOverride();
    assertFalse(PipelineClock.isOverridden());
    assertEquals(LocalDate.now().getYear(), PipelineClock.currentYear());
  }

  @Test void setOverrideWithNullClears() {
    PipelineClock.setOverrideForTest(LocalDate.of(1999, 7, 4));
    PipelineClock.setOverride(null);
    assertFalse(PipelineClock.isOverridden());
    assertEquals(LocalDate.now().getYear(), PipelineClock.currentYear());
  }
}
