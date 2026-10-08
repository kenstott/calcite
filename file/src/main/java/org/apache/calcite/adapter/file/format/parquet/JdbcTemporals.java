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

import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneOffset;

/**
 * Reads the calendar date or wall-clock time out of a JDBC temporal object.
 *
 * <p>A {@link Date} or {@link Timestamp} that a JDBC driver returns for a column without a
 * time zone is built in the JVM's default zone: its epoch milliseconds are those of the
 * local midnight, or the local wall-clock time, of the value. Reading those milliseconds as
 * UTC moves the value by the zone's offset, which for a date is a day east of Greenwich.
 * The methods here read the value back through the same zone it was built in, so the result
 * does not depend on which zone that is.
 */
final class JdbcTemporals {
  private JdbcTemporals() {
  }

  /** The days since 1970-01-01 of the calendar date a JDBC date holds. */
  static int epochDay(Date date) {
    return Math.toIntExact(date.toLocalDate().toEpochDay());
  }

  /**
   * The wall-clock time a JDBC timestamp of a column without a time zone holds, as the
   * milliseconds since the epoch of that same wall-clock time in UTC.
   */
  static long wallClockMillis(Timestamp timestamp) {
    return timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
  }
}
