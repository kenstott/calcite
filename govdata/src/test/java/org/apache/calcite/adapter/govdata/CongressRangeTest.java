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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests {@link CongressRange}: calendar years map to the Congress that covers them. */
@Tag("unit")
class CongressRangeTest {

  @AfterEach void clearProperties() {
    System.clearProperty("GOVDATA_START_CONGRESS");
    System.clearProperty("GOVDATA_END_CONGRESS");
  }

  @Test void yearsMapToTheCoveringCongress() {
    // The 1st Congress began in 1789; each Congress spans two calendar years.
    assertEquals(1, CongressRange.congressOf(1789));
    assertEquals(1, CongressRange.congressOf(1790));
    assertEquals(2, CongressRange.congressOf(1791));
    assertEquals(111, CongressRange.congressOf(2009));
    assertEquals(111, CongressRange.congressOf(2010));
    assertEquals(119, CongressRange.congressOf(2025));
    assertEquals(119, CongressRange.congressOf(2026));
    assertEquals(120, CongressRange.congressOf(2027));
  }

  @Test void startAndEndYearBecomeTheCongressRangeProperties() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("startYear", 2010);
    operand.put("endYear", "2026");
    CongressRange.deriveEarlyProperties(operand);
    assertEquals("111", System.getProperty("GOVDATA_START_CONGRESS"));
    assertEquals("119", System.getProperty("GOVDATA_END_CONGRESS"));
  }

  @Test void eachYearSetsOnlyItsOwnCongress() {
    Map<String, Object> startOnly = new HashMap<String, Object>();
    startOnly.put("startYear", 2010);
    CongressRange.deriveEarlyProperties(startOnly);
    assertEquals("111", System.getProperty("GOVDATA_START_CONGRESS"));
    assertNull(System.getProperty("GOVDATA_END_CONGRESS"));

    System.clearProperty("GOVDATA_START_CONGRESS");
    Map<String, Object> endOnly = new HashMap<String, Object>();
    endOnly.put("endYear", 2025);
    CongressRange.deriveEarlyProperties(endOnly);
    assertNull(System.getProperty("GOVDATA_START_CONGRESS"));
    assertEquals("119", System.getProperty("GOVDATA_END_CONGRESS"));
  }

  @Test void nothingIsSetWithoutEitherYear() {
    CongressRange.deriveEarlyProperties(new HashMap<String, Object>());
    assertNull(System.getProperty("GOVDATA_START_CONGRESS"));
    assertNull(System.getProperty("GOVDATA_END_CONGRESS"));
  }
}
