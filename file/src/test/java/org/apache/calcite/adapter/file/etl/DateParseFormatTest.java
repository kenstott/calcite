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
import java.time.format.DateTimeParseException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link DateParseFormat#parse(String)} — the pure-Java fallback parser, checked
 * against the same values verified live against DuckDB's {@code TRY_STRPTIME}/{@code TRY_CAST}
 * behavior for {@link DateParseFormat#toExpression(String)} during development, so the two
 * implementations stay in lockstep.
 */
@Tag("unit")
class DateParseFormatTest {

  @Test void testSlash() {
    assertEquals(LocalDate.of(2024, 12, 31), DateParseFormat.SLASH.parse("12/31/2024"));
  }

  @Test void testSlashShortAcceptsSingleDigits() {
    assertEquals(LocalDate.of(2024, 1, 5), DateParseFormat.SLASH_SHORT.parse("1/5/2024"));
  }

  @Test void testSlashShortYearPivot() {
    // POSIX/strptime %y pivot: 00-68 -> 2000-2068, 69-99 -> 1969-1999.
    assertEquals(LocalDate.of(2024, 12, 31), DateParseFormat.SLASH_SHORT_YEAR.parse("12/31/24"));
    assertEquals(LocalDate.of(1969, 1, 1), DateParseFormat.SLASH_SHORT_YEAR.parse("01/01/69"));
    assertEquals(LocalDate.of(2068, 1, 1), DateParseFormat.SLASH_SHORT_YEAR.parse("01/01/68"));
  }

  @Test void testMmddyyyyPadsShortInput() {
    assertEquals(LocalDate.of(2024, 12, 31), DateParseFormat.MMDDYYYY.parse("12312024"));
    assertEquals(LocalDate.of(2024, 1, 31), DateParseFormat.MMDDYYYY.parse("1312024"));
  }

  @Test void testYyyymmdd() {
    assertEquals(LocalDate.of(2024, 12, 31), DateParseFormat.YYYYMMDD.parse("20241231"));
  }

  @Test void testIso() {
    assertEquals(LocalDate.of(2024, 12, 31), DateParseFormat.ISO.parse("2024-12-31"));
  }

  @Test void testYyyySlashMmSlashDd() {
    assertEquals(
        LocalDate.of(2024, 12, 31), DateParseFormat.YYYY_SLASH_MM_SLASH_DD.parse("2024/12/31"));
  }

  @Test void testDdSlashMmSlashYyyy() {
    assertEquals(
        LocalDate.of(2024, 12, 31), DateParseFormat.DD_SLASH_MM_SLASH_YYYY.parse("31/12/2024"));
  }

  @Test void testDdDashMmDashYyyy() {
    assertEquals(
        LocalDate.of(2024, 12, 31), DateParseFormat.DD_DASH_MM_DASH_YYYY.parse("31-12-2024"));
  }

  @Test void testDdMonYyIsCaseInsensitiveWithPivot() {
    assertEquals(LocalDate.of(2024, 9, 27), DateParseFormat.DD_MON_YY.parse("27-SEP-24"));
    assertEquals(LocalDate.of(2024, 9, 27), DateParseFormat.DD_MON_YY.parse("27-sep-24"));
    assertEquals(LocalDate.of(1969, 9, 27), DateParseFormat.DD_MON_YY.parse("27-sep-69"));
  }

  @Test void testDdMonYyyy() {
    assertEquals(LocalDate.of(2024, 9, 27), DateParseFormat.DD_MON_YYYY.parse("27-SEP-2024"));
  }

  @Test void testDdMonthYyyyFullName() {
    assertEquals(
        LocalDate.of(2024, 9, 27), DateParseFormat.DD_MONTH_YYYY.parse("27-September-2024"));
  }

  @Test void testMonDdYyyy() {
    assertEquals(LocalDate.of(2024, 9, 27), DateParseFormat.MON_DD_YYYY.parse("Sep 27 2024"));
  }

  @Test void testMonthDdYyyyFullName() {
    assertEquals(
        LocalDate.of(2024, 9, 27), DateParseFormat.MONTH_DD_YYYY.parse("September 27 2024"));
  }

  @Test void testEpochSecondsIsUtcCalendarDate() {
    assertEquals(LocalDate.of(2024, 12, 31), DateParseFormat.EPOCH_SECONDS.parse("1735603200"));
  }

  @Test void testEpochMillisIsUtcCalendarDate() {
    assertEquals(
        LocalDate.of(2024, 12, 31), DateParseFormat.EPOCH_MILLIS.parse("1735603200000"));
  }

  @Test void testFiscalQuarterComputesStartMonth() {
    assertEquals(LocalDate.of(2024, 7, 1), DateParseFormat.FISCAL_QUARTER.parse("2024-Q3"));
    assertEquals(LocalDate.of(2024, 1, 1), DateParseFormat.FISCAL_QUARTER.parse("2024-Q1"));
  }

  @Test void testMmddyyyyOrSlashBranchesOnSlash() {
    assertEquals(
        LocalDate.of(2024, 12, 31), DateParseFormat.MMDDYYYY_OR_SLASH.parse("12/31/2024"));
    assertEquals(
        LocalDate.of(2024, 12, 31), DateParseFormat.MMDDYYYY_OR_SLASH.parse("12312024"));
  }

  /**
   * Verified against live DuckDB: any offset/Z suffix is discarded, not converted — the
   * result is the literal date digits as written, matching {@code toExpression}'s documented
   * caveat exactly.
   */
  @Test void testIsoDatetimeDiscardsOffsetAndHandlesBothSeparators() {
    assertEquals(LocalDate.of(2024, 12, 31),
        DateParseFormat.ISO_DATETIME.parse("2024-12-31T23:30:00+05:00"));
    assertEquals(LocalDate.of(2024, 12, 31),
        DateParseFormat.ISO_DATETIME.parse("2024-12-31 00:00:00"));
    assertEquals(LocalDate.of(2024, 12, 31),
        DateParseFormat.ISO_DATETIME.parse("2024-12-31T00:00:00Z"));
    assertEquals(LocalDate.of(2024, 12, 31), DateParseFormat.ISO_DATETIME.parse("2024-12-31"));
  }

  /** Same discard-offset semantics as ISO_DATETIME — verified against live DuckDB. */
  @Test void testTimestampToDateDiscardsOffsetAndHandlesBothSeparators() {
    assertEquals(LocalDate.of(2024, 12, 31),
        DateParseFormat.TIMESTAMP_TO_DATE.parse("2024-12-31T23:30:00+05:00"));
    assertEquals(LocalDate.of(2024, 12, 31),
        DateParseFormat.TIMESTAMP_TO_DATE.parse("2024-12-31 23:30:00"));
    assertEquals(LocalDate.of(2024, 12, 31),
        DateParseFormat.TIMESTAMP_TO_DATE.parse("2024-12-31T23:30:00Z"));
    assertEquals(LocalDate.of(2024, 12, 31), DateParseFormat.TIMESTAMP_TO_DATE.parse("2024-12-31"));
  }

  @Test void testNullAndBlankAreNullNotExceptions() {
    for (DateParseFormat fmt : DateParseFormat.values()) {
      assertNull(fmt.parse(null), fmt + " should return null for null input");
      assertNull(fmt.parse(""), fmt + " should return null for empty input");
      assertNull(fmt.parse("   "), fmt + " should return null for blank input");
    }
  }

  @Test void testGenuineMismatchThrowsRatherThanSilentlyNulling() {
    assertThrows(DateTimeParseException.class, () -> DateParseFormat.SLASH.parse("not-a-date"));
    assertThrows(DateTimeParseException.class, () -> DateParseFormat.ISO.parse("31/12/2024"));
    assertThrows(RuntimeException.class, () -> DateParseFormat.EPOCH_SECONDS.parse("not-a-number"));
    assertThrows(RuntimeException.class, () -> DateParseFormat.FISCAL_QUARTER.parse("2024-07"));
  }

  /** Every format's Java parser and DuckDB expression must exist and be independently callable. */
  @Test void testEveryFormatHasBothImplementations() {
    for (DateParseFormat fmt : DateParseFormat.values()) {
      String expr = fmt.toExpression("col");
      org.junit.jupiter.api.Assertions.assertNotNull(expr, fmt + " toExpression must not be null");
      org.junit.jupiter.api.Assertions.assertTrue(expr.contains("col"),
          fmt + " toExpression must reference the column");
    }
  }
}
