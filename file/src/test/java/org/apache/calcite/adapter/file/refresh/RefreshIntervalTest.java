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
 * Tests for {@link RefreshInterval}.
 */
@Tag("unit")
class RefreshIntervalTest {

  @Test void testParseNull() {
    assertNull(RefreshInterval.parse(null));
  }

  @Test void testParseEmptyString() {
    assertNull(RefreshInterval.parse(""));
  }

  @Test void testParseWhitespace() {
    assertNull(RefreshInterval.parse("   "));
  }

  @Test void testParseSeconds() {
    Duration result = RefreshInterval.parse("30 seconds");
    assertNotNull(result);
    assertEquals(30, result.getSeconds());
  }

  @Test void testParseSecondSingular() {
    Duration result = RefreshInterval.parse("1 second");
    assertNotNull(result);
    assertEquals(1, result.getSeconds());
  }

  @Test void testParseMinutes() {
    Duration result = RefreshInterval.parse("5 minutes");
    assertNotNull(result);
    assertEquals(Duration.ofMinutes(5), result);
  }

  @Test void testParseMinuteSingular() {
    Duration result = RefreshInterval.parse("1 minute");
    assertNotNull(result);
    assertEquals(Duration.ofMinutes(1), result);
  }

  @Test void testParseHours() {
    Duration result = RefreshInterval.parse("2 hours");
    assertNotNull(result);
    assertEquals(Duration.ofHours(2), result);
  }

  @Test void testParseHourSingular() {
    Duration result = RefreshInterval.parse("1 hour");
    assertNotNull(result);
    assertEquals(Duration.ofHours(1), result);
  }

  @Test void testParseDays() {
    Duration result = RefreshInterval.parse("3 days");
    assertNotNull(result);
    assertEquals(Duration.ofDays(3), result);
  }

  @Test void testParseDaySingular() {
    Duration result = RefreshInterval.parse("1 day");
    assertNotNull(result);
    assertEquals(Duration.ofDays(1), result);
  }

  @Test void testParseCaseInsensitive() {
    Duration result = RefreshInterval.parse("5 MINUTES");
    assertNotNull(result);
    assertEquals(Duration.ofMinutes(5), result);
  }

  @Test void testParseWithLeadingTrailingSpaces() {
    Duration result = RefreshInterval.parse("  10 seconds  ");
    assertNotNull(result);
    assertEquals(Duration.ofSeconds(10), result);
  }

  @Test void testParseIso8601Seconds() {
    Duration result = RefreshInterval.parse("PT1S");
    assertNotNull(result);
    assertEquals(Duration.ofSeconds(1), result);
  }

  @Test void testParseIso8601Minutes() {
    Duration result = RefreshInterval.parse("PT5M");
    assertNotNull(result);
    assertEquals(Duration.ofMinutes(5), result);
  }

  @Test void testParseIso8601Hours() {
    Duration result = RefreshInterval.parse("PT1H");
    assertNotNull(result);
    assertEquals(Duration.ofHours(1), result);
  }

  @Test void testParseInvalidFormat() {
    assertNull(RefreshInterval.parse("not a duration"));
  }

  @Test void testParseInvalidIso8601() {
    // "Pxyz" is invalid ISO 8601 and not a valid human-readable format
    assertNull(RefreshInterval.parse("Pxyz"));
  }

  @Test void testGetEffectiveIntervalTablePrecedence() {
    Duration result = RefreshInterval.getEffectiveInterval("5 minutes", "1 hour");
    assertNotNull(result);
    assertEquals(Duration.ofMinutes(5), result);
  }

  @Test void testGetEffectiveIntervalFallbackToSchema() {
    Duration result = RefreshInterval.getEffectiveInterval(null, "1 hour");
    assertNotNull(result);
    assertEquals(Duration.ofHours(1), result);
  }

  @Test void testGetEffectiveIntervalBothNull() {
    assertNull(RefreshInterval.getEffectiveInterval(null, null));
  }

  @Test void testGetEffectiveIntervalTableInvalid() {
    Duration result = RefreshInterval.getEffectiveInterval("invalid", "1 hour");
    assertNotNull(result);
    assertEquals(Duration.ofHours(1), result);
  }

  @Test void testGetEffectiveIntervalBothInvalid() {
    assertNull(RefreshInterval.getEffectiveInterval("invalid", "also invalid"));
  }

  @Test void testGetEffectiveIntervalTableEmpty() {
    Duration result = RefreshInterval.getEffectiveInterval("", "30 seconds");
    assertNotNull(result);
    assertEquals(Duration.ofSeconds(30), result);
  }
}
