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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Year;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the empty-marker high-water-mark settlement logic in
 * {@link S3HivePipelineTracker} (the decisioning that lets a lagged source self-heal:
 * an empty period above the published frontier stays pending and is re-fetched, while an
 * empty period at/below the frontier — or aged past the recency horizon — settles).
 *
 * <p>Exercises the private {@code classifySettledEmpties}/{@code yearOf} directly via
 * reflection, with no S3 or DuckDB interaction (mirrors the coverage-test style).
 */
@Tag("unit")
public class S3HivePipelineTrackerEmptyHwmTest {

  /** Default recency horizon (govdata.tracker.emptyRecencyHorizonYears). */
  private static final int HORIZON = 3;

  private static S3HivePipelineTracker newTracker() {
    return new S3HivePipelineTracker("s3://bucket/tracker", "http://localhost:9000");
  }

  private static String periodKey(int year) {
    return "type=ncvs__year=" + year;
  }

  @SuppressWarnings("unchecked")
  private static List<String> classify(S3HivePipelineTracker t,
      List<String> emptyKeys, Set<String> completeKeys) throws Exception {
    Method m = S3HivePipelineTracker.class.getDeclaredMethod(
        "classifySettledEmpties", List.class, Set.class);
    m.setAccessible(true);
    return (List<String>) m.invoke(t, emptyKeys, completeKeys);
  }

  private static int yearOf(S3HivePipelineTracker t, String key) throws Exception {
    Method m = S3HivePipelineTracker.class.getDeclaredMethod("yearOf", String.class);
    m.setAccessible(true);
    return (Integer) m.invoke(t, key);
  }

  @Test
  void yearOfExtractsPeriodYearAndDefaultsToZero() throws Exception {
    S3HivePipelineTracker t = newTracker();
    try {
      assertEquals(2024, yearOf(t, periodKey(2024)));
      assertEquals(0, yearOf(t, "type=trends"));   // no period component
    } finally {
      t.close();
    }
  }

  @Test
  void emptyBelowHighWaterMarkSettles() throws Exception {
    S3HivePipelineTracker t = newTracker();
    try {
      Set<String> complete = new HashSet<String>();
      complete.add(periodKey(2023));               // HWM = 2023 (data exists)
      List<String> empties = new ArrayList<String>();
      empties.add(periodKey(2022));                // below the frontier → genuinely empty
      List<String> settled = classify(t, empties, complete);
      assertEquals(1, settled.size());
      assertTrue(settled.contains(periodKey(2022)));
    } finally {
      t.close();
    }
  }

  @Test
  void emptyAboveHighWaterMarkStaysPending() throws Exception {
    S3HivePipelineTracker t = newTracker();
    try {
      int currentYear = Year.now(ZoneOffset.UTC).getValue();
      Set<String> complete = new HashSet<String>();
      complete.add(periodKey(currentYear - 2));    // newest published period
      List<String> empties = new ArrayList<String>();
      empties.add(periodKey(currentYear));         // not yet published → above the frontier
      List<String> settled = classify(t, empties, complete);
      assertTrue(settled.isEmpty(), "current-year empty above HWM must remain pending");
    } finally {
      t.close();
    }
  }

  @Test
  void emptyAgedPastHorizonSettlesWithoutHighWaterMark() throws Exception {
    S3HivePipelineTracker t = newTracker();
    try {
      int currentYear = Year.now(ZoneOffset.UTC).getValue();
      Set<String> complete = new HashSet<String>();   // no data anywhere → HWM = 0
      List<String> empties = new ArrayList<String>();
      empties.add(periodKey(currentYear - (HORIZON + 2))); // well past the horizon
      empties.add(periodKey(currentYear));                 // recent → still pending
      List<String> settled = classify(t, empties, complete);
      assertEquals(1, settled.size());
      assertTrue(settled.contains(periodKey(currentYear - (HORIZON + 2))));
      assertFalse(settled.contains(periodKey(currentYear)));
    } finally {
      t.close();
    }
  }

  @Test
  void nonPeriodEmptySettlesImmediately() throws Exception {
    S3HivePipelineTracker t = newTracker();
    try {
      List<String> empties = new ArrayList<String>();
      empties.add("type=trends");                  // no period → no frontier concept
      List<String> settled = classify(t, empties, new HashSet<String>());
      assertEquals(1, settled.size());
    } finally {
      t.close();
    }
  }

  @Test
  void emptyListReturnsEmpty() throws Exception {
    S3HivePipelineTracker t = newTracker();
    try {
      assertTrue(classify(t, new ArrayList<String>(), new HashSet<String>()).isEmpty());
    } finally {
      t.close();
    }
  }
}
