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
package org.apache.calcite.adapter.file.format.parquet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A date or a time without a zone is stored as written, whatever zone the JVM is set to.
 */
@Tag("unit")
@Isolated("sets the JVM's default time zone")
class JdbcTemporalsTest {
  /** East of Greenwich by more than twelve hours, west of it, and neither. */
  private static final String[] ZONES = {"Pacific/Chatham", "America/Los_Angeles", "UTC"};

  private TimeZone original;

  @BeforeEach void rememberZone() {
    original = TimeZone.getDefault();
  }

  @AfterEach void restoreZone() {
    TimeZone.setDefault(original);
  }

  @Test void aDateIsItsCalendarDateInEveryZone() {
    long expected = LocalDate.of(2024, 1, 15).toEpochDay();
    for (String zone : ZONES) {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      assertEquals(expected, JdbcTemporals.epochDay(Date.valueOf("2024-01-15")), zone);
    }
  }

  @Test void aDateBeforeTheEpochIsItsCalendarDateInEveryZone() {
    long expected = LocalDate.of(1969, 12, 31).toEpochDay();
    for (String zone : ZONES) {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      assertEquals(expected, JdbcTemporals.epochDay(Date.valueOf("1969-12-31")), zone);
    }
  }

  @Test void aTimestampWithoutAZoneIsItsWallClockTimeInEveryZone() {
    // 1996-08-02 00:01:02 read as UTC
    long expected = 838944062000L;
    for (String zone : ZONES) {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      assertEquals(expected,
          JdbcTemporals.wallClockMillis(Timestamp.valueOf("1996-08-02 00:01:02")), zone);
    }
  }

  @Test void aTimestampKeepsItsMilliseconds() {
    for (String zone : ZONES) {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      assertEquals(838944062123L,
          JdbcTemporals.wallClockMillis(Timestamp.valueOf("1996-08-02 00:01:02.123")), zone);
    }
  }
}
