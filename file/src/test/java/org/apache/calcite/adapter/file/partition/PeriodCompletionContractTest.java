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
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asserts that every persistent tracker really implements the period-completion API.
 *
 * <p>{@link IncrementalTracker} supplies defaults for the three period methods, and they are
 * deliberately inert: {@code markPeriodComplete} does nothing and {@code isPeriodComplete} returns
 * false, documented as correct "for non-persistent trackers", which cannot prove a period complete.
 *
 * <p>A persistent tracker that simply fails to override them inherits that inertness without any
 * signal. {@link PGPipelineTracker} did, and the consequences were silent and total: nothing was
 * ever written, so {@code isPeriodComplete} could only answer false, so the per-period skip never
 * fired and every {@code lookbackPeriods} declaration in every schema reopened nothing on every
 * run. Nothing failed, no row was wrong, and the pipeline logged that it had marked periods
 * complete — the call was real, only its effect was not.
 *
 * <p>Unit tests did not catch it because they run against a stub tracker that implements the API,
 * so the feature worked in every test and in none of production. This asserts on the class the ETL
 * actually runs with, which is the only place the gap was visible.
 */
@Tag("unit")
public class PeriodCompletionContractTest {

  /** The methods a tracker must own itself rather than inherit as a no-op. */
  private static final String[] PERIOD_METHODS = {
      "markPeriodComplete", "isPeriodComplete", "invalidatePeriod"};

  private static Map<String, String> period(String year) {
    Map<String, String> p = new LinkedHashMap<String, String>();
    p.put("year", year);
    return p;
  }

  private static Method declared(Class<?> type, String name) {
    for (Method m : type.getDeclaredMethods()) {
      if (m.getName().equals(name)) {
        return m;
      }
    }
    return null;
  }

  /**
   * The regression this exists for. Inheriting the interface default leaves a persistent tracker
   * unable to answer the one question the lookback is built on.
   */
  @Test void postgresTrackerDeclaresEveryPeriodMethodItself() {
    for (String name : PERIOD_METHODS) {
      assertNotNull(declared(PGPipelineTracker.class, name),
          "PGPipelineTracker must declare " + name + " — inheriting IncrementalTracker's default "
              + "makes it a silent no-op, which disables per-period skipping and every lookback");
    }
  }

  /**
   * The key must distinguish periods, or two of them collide on one marker and a lookback cannot
   * tell which was published.
   */
  @Test void periodKeyDistinguishesPeriodsAndPipelines() {
    String y2024 = IncrementalTracker.periodCompletionKey("t", period("2024"));
    String y2025 = IncrementalTracker.periodCompletionKey("t", period("2025"));
    String other = IncrementalTracker.periodCompletionKey("other", period("2024"));

    assertTrue(!y2024.equals(y2025), "two years must not share a marker");
    assertTrue(!y2024.equals(other), "two pipelines must not share a marker");
    assertTrue(y2024.endsWith("t"), "key carries the pipeline name");
  }

  /** Unset slots collapse to a fixed placeholder, so a year-only table keys stably. */
  @Test void unsetSlotsAreStableNotNull() {
    String a = IncrementalTracker.periodCompletionKey("t", period("2024"));
    String b = IncrementalTracker.periodCompletionKey("t", period("2024"));
    assertEquals(a, b, "the same period must always produce the same key");
    assertTrue(a.contains("NA"), "absent slots use the NA placeholder rather than being dropped");
  }

  /**
   * A table with no period slot at all must not be period-tracked: its key would be entirely
   * placeholders, so every such combo for a pipeline would collide on one marker.
   */
  @Test void aComboWithNoPeriodSlotIsNotPeriodTracked() {
    Map<String, String> noPeriod = new LinkedHashMap<String, String>();
    noPeriod.put("state", "CA");
    assertTrue(!IncrementalTracker.hasCanonicalPeriod(noPeriod),
        "without a period slot the key is all-NA and every combo would share one marker");
  }
}
