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
package org.apache.calcite.adapter.file.temporal;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link LocalTimestamp} and {@link UtcTimestamp}.
 * Uses computed epoch millis to avoid timezone-dependent assertions.
 */
@Tag("unit")
class TemporalTimestampTest {

  private static final DateTimeFormatter FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  /** Helper: compute UTC epoch millis for a given UTC datetime. */
  private static long utcMillis(int year, int month, int day, int hour, int min, int sec) {
    return LocalDateTime.of(year, month, day, hour, min, sec)
        .toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  /** Helper: format millis as UTC string using same approach as production code. */
  private static String formatUtc(long millis) {
    Instant instant = Instant.ofEpochMilli(millis);
    LocalDateTime utcDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
    return FMT.format(utcDateTime);
  }

  @Test void testLocalTimestampToString() {
    long millis = utcMillis(2024, 1, 15, 10, 30, 0);
    LocalTimestamp ts = new LocalTimestamp(millis);
    String result = ts.toString();
    assertEquals(formatUtc(millis), result);
  }

  @Test void testLocalTimestampToUtcString() {
    long millis = utcMillis(2024, 1, 15, 10, 30, 0);
    LocalTimestamp ts = new LocalTimestamp(millis);
    assertEquals(formatUtc(millis), ts.toUTCString());
  }

  @Test void testLocalTimestampGetOriginalUtcTime() {
    long millis = utcMillis(2024, 6, 15, 12, 0, 0);
    LocalTimestamp ts = new LocalTimestamp(millis);
    assertEquals(millis, ts.getOriginalUtcTime());
  }

  @Test void testLocalTimestampEpochZero() {
    LocalTimestamp ts = new LocalTimestamp(0L);
    assertEquals(formatUtc(0L), ts.toString());
    assertEquals(0L, ts.getOriginalUtcTime());
  }

  @Test void testLocalTimestampIsTimestamp() {
    LocalTimestamp ts = new LocalTimestamp(utcMillis(2024, 1, 1, 0, 0, 0));
    assertTrue(ts instanceof Timestamp);
  }

  @Test void testUtcTimestampToString() {
    long millis = utcMillis(2024, 1, 15, 10, 30, 0);
    UtcTimestamp ts = new UtcTimestamp(millis);
    assertEquals(formatUtc(millis), ts.toString());
  }

  @Test void testUtcTimestampIsTimestamp() {
    UtcTimestamp ts = new UtcTimestamp(utcMillis(2024, 1, 1, 0, 0, 0));
    assertTrue(ts instanceof Timestamp);
  }

  @Test void testUtcTimestampEpochZero() {
    UtcTimestamp ts = new UtcTimestamp(0L);
    assertEquals(formatUtc(0L), ts.toString());
  }

  @Test void testUtcTimestampEqualsWithSameValue() {
    long millis = utcMillis(2024, 3, 1, 8, 0, 0);
    UtcTimestamp ts1 = new UtcTimestamp(millis);
    UtcTimestamp ts2 = new UtcTimestamp(millis);
    assertEquals(ts1, ts2);
  }

  @Test void testUtcTimestampNotEqualsWithDifferentValue() {
    long m1 = utcMillis(2024, 3, 1, 8, 0, 0);
    long m2 = utcMillis(2024, 3, 1, 8, 0, 1);
    UtcTimestamp ts1 = new UtcTimestamp(m1);
    UtcTimestamp ts2 = new UtcTimestamp(m2);
    assertNotEquals(ts1, ts2);
  }

  @Test void testUtcTimestampEqualsWithRegularTimestamp() {
    long millis = utcMillis(2024, 3, 1, 8, 0, 0);
    UtcTimestamp ts1 = new UtcTimestamp(millis);
    Timestamp ts2 = new Timestamp(millis);
    assertEquals(ts1, ts2);
  }

  @Test void testUtcTimestampHashCode() {
    long millis = utcMillis(2024, 3, 1, 8, 0, 0);
    UtcTimestamp ts1 = new UtcTimestamp(millis);
    UtcTimestamp ts2 = new UtcTimestamp(millis);
    assertEquals(ts1.hashCode(), ts2.hashCode());
  }

  @Test void testUtcTimestampEqualsSelf() {
    UtcTimestamp ts = new UtcTimestamp(utcMillis(2024, 1, 1, 0, 0, 0));
    assertEquals(ts, ts);
  }

  @Test void testUtcTimestampNotEqualsNull() {
    UtcTimestamp ts = new UtcTimestamp(utcMillis(2024, 1, 1, 0, 0, 0));
    assertNotEquals(null, ts);
  }

  @Test void testUtcTimestampNotEqualsNonTimestamp() {
    UtcTimestamp ts = new UtcTimestamp(utcMillis(2024, 1, 1, 0, 0, 0));
    assertNotEquals("not a timestamp", ts);
  }

  @Test void testLocalTimestampConsistentFormats() {
    long millis = utcMillis(2024, 7, 4, 15, 30, 45);
    LocalTimestamp ts = new LocalTimestamp(millis);
    assertEquals(ts.toString(), ts.toUTCString());
  }

  @Test void testTimestampAtEndOfDay() {
    long millis = utcMillis(2024, 1, 15, 23, 59, 59);
    LocalTimestamp local = new LocalTimestamp(millis);
    UtcTimestamp utc = new UtcTimestamp(millis);
    String expected = formatUtc(millis);
    assertEquals(expected, local.toString());
    assertEquals(expected, utc.toString());
  }

  @Test void testTimestampAtStartOfDay() {
    long millis = utcMillis(2024, 1, 15, 0, 0, 0);
    LocalTimestamp local = new LocalTimestamp(millis);
    UtcTimestamp utc = new UtcTimestamp(millis);
    String expected = formatUtc(millis);
    assertEquals(expected, local.toString());
    assertEquals(expected, utc.toString());
  }

  @Test void testGetTimeReturnsStoredValue() {
    long millis = utcMillis(2024, 6, 1, 12, 0, 0);
    UtcTimestamp ts = new UtcTimestamp(millis);
    assertEquals(millis, ts.getTime());
  }

  @Test void testLocalTimestampNotNull() {
    LocalTimestamp ts = new LocalTimestamp(utcMillis(2024, 1, 1, 0, 0, 0));
    assertNotNull(ts.toString());
    assertNotNull(ts.toUTCString());
  }

  @Test void testToStringContainsYear() {
    long millis = utcMillis(2024, 6, 15, 12, 0, 0);
    LocalTimestamp ts = new LocalTimestamp(millis);
    assertTrue(ts.toString().contains("2024"));
  }

  @Test void testToStringContainsMonth() {
    long millis = utcMillis(2024, 12, 25, 0, 0, 0);
    LocalTimestamp ts = new LocalTimestamp(millis);
    assertTrue(ts.toString().contains("12-25"));
  }

  @Test void testUtcTimestampToStringContainsTime() {
    long millis = utcMillis(2024, 1, 1, 14, 30, 45);
    UtcTimestamp ts = new UtcTimestamp(millis);
    assertTrue(ts.toString().contains("14:30:45"));
  }
}
