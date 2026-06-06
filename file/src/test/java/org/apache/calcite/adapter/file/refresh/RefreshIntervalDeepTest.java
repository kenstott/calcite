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
package org.apache.calcite.adapter.file.refresh;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Deep coverage tests for {@link RefreshInterval} covering all parsing branches:
 * human-readable formats, ISO 8601, null/empty handling, plural/singular,
 * case sensitivity, and getEffectiveInterval inheritance.
 */
@Tag("unit")
class RefreshIntervalDeepTest {

  // --- parse: null and empty ---

  @Test void testParseNull() {
    assertNull(RefreshInterval.parse(null));
  }

  @Test void testParseEmptyString() {
    assertNull(RefreshInterval.parse(""));
  }

  @Test void testParseWhitespaceOnly() {
    assertNull(RefreshInterval.parse("   "));
  }

  // --- parse: human-readable seconds ---

  @Test void testParseSecondsSingular() {
    Duration d = RefreshInterval.parse("1 second");
    assertNotNull(d);
    assertEquals(1, d.getSeconds());
  }

  @Test void testParseSecondsPlural() {
    Duration d = RefreshInterval.parse("30 seconds");
    assertNotNull(d);
    assertEquals(30, d.getSeconds());
  }

  // --- parse: human-readable minutes ---

  @Test void testParseMinuteSingular() {
    Duration d = RefreshInterval.parse("1 minute");
    assertNotNull(d);
    assertEquals(60, d.getSeconds());
  }

  @Test void testParseMinutesPlural() {
    Duration d = RefreshInterval.parse("5 minutes");
    assertNotNull(d);
    assertEquals(300, d.getSeconds());
  }

  // --- parse: human-readable hours ---

  @Test void testParseHourSingular() {
    Duration d = RefreshInterval.parse("1 hour");
    assertNotNull(d);
    assertEquals(3600, d.getSeconds());
  }

  @Test void testParseHoursPlural() {
    Duration d = RefreshInterval.parse("2 hours");
    assertNotNull(d);
    assertEquals(7200, d.getSeconds());
  }

  // --- parse: human-readable days ---

  @Test void testParseDaySingular() {
    Duration d = RefreshInterval.parse("1 day");
    assertNotNull(d);
    assertEquals(86400, d.getSeconds());
  }

  @Test void testParseDaysPlural() {
    Duration d = RefreshInterval.parse("3 days");
    assertNotNull(d);
    assertEquals(259200, d.getSeconds());
  }

  // --- parse: case insensitivity ---

  @Test void testParseCaseInsensitive() {
    Duration d = RefreshInterval.parse("5 MINUTES");
    assertNotNull(d);
    assertEquals(300, d.getSeconds());
  }

  @Test void testParseMixedCase() {
    Duration d = RefreshInterval.parse("10 Seconds");
    assertNotNull(d);
    assertEquals(10, d.getSeconds());
  }

  // --- parse: ISO 8601 format ---

  @Test void testParseIso8601Seconds() {
    Duration d = RefreshInterval.parse("PT1S");
    assertNotNull(d);
    assertEquals(1, d.getSeconds());
  }

  @Test void testParseIso8601Minutes() {
    Duration d = RefreshInterval.parse("PT5M");
    assertNotNull(d);
    assertEquals(300, d.getSeconds());
  }

  @Test void testParseIso8601Hours() {
    Duration d = RefreshInterval.parse("PT2H");
    assertNotNull(d);
    assertEquals(7200, d.getSeconds());
  }

  @Test void testParseIso8601Complex() {
    Duration d = RefreshInterval.parse("PT1H30M");
    assertNotNull(d);
    assertEquals(5400, d.getSeconds());
  }

  @Test void testParseIso8601Days() {
    Duration d = RefreshInterval.parse("P1D");
    assertNotNull(d);
    assertEquals(86400, d.getSeconds());
  }

  // --- parse: invalid formats ---

  @Test void testParseInvalidFormat() {
    assertNull(RefreshInterval.parse("not a duration"));
  }

  @Test void testParsePartialMatch() {
    assertNull(RefreshInterval.parse("5 weeks")); // weeks not supported
  }

  @Test void testParseInvalidIso() {
    // Invalid ISO format - falls through to human-readable (also invalid)
    assertNull(RefreshInterval.parse("PXYZ"));
  }

  // --- parse: whitespace handling ---

  @Test void testParseWithLeadingTrailingWhitespace() {
    Duration d = RefreshInterval.parse("  5 minutes  ");
    assertNotNull(d);
    assertEquals(300, d.getSeconds());
  }

  // --- getEffectiveInterval ---

  @Test void testGetEffectiveIntervalTableOverridesSchema() {
    Duration d = RefreshInterval.getEffectiveInterval("1 minute", "5 minutes");
    assertNotNull(d);
    assertEquals(60, d.getSeconds());
  }

  @Test void testGetEffectiveIntervalSchemaFallback() {
    Duration d = RefreshInterval.getEffectiveInterval(null, "5 minutes");
    assertNotNull(d);
    assertEquals(300, d.getSeconds());
  }

  @Test void testGetEffectiveIntervalBothNull() {
    assertNull(RefreshInterval.getEffectiveInterval(null, null));
  }

  @Test void testGetEffectiveIntervalTableInvalidSchemaValid() {
    Duration d = RefreshInterval.getEffectiveInterval("invalid", "5 minutes");
    assertNotNull(d);
    assertEquals(300, d.getSeconds());
  }

  @Test void testGetEffectiveIntervalBothInvalid() {
    assertNull(RefreshInterval.getEffectiveInterval("invalid", "also invalid"));
  }

  @Test void testGetEffectiveIntervalTableEmptySchemaValid() {
    Duration d = RefreshInterval.getEffectiveInterval("", "10 seconds");
    assertNotNull(d);
    assertEquals(10, d.getSeconds());
  }

  @Test void testGetEffectiveIntervalTableValidSchemaNull() {
    Duration d = RefreshInterval.getEffectiveInterval("1 hour", null);
    assertNotNull(d);
    assertEquals(3600, d.getSeconds());
  }

  // --- Large values ---

  @Test void testParseLargeValue() {
    Duration d = RefreshInterval.parse("999 days");
    assertNotNull(d);
    assertEquals(999L * 86400, d.getSeconds());
  }

  @Test void testParseZeroSeconds() {
    Duration d = RefreshInterval.parse("0 seconds");
    assertNotNull(d);
    assertEquals(0, d.getSeconds());
  }
}
